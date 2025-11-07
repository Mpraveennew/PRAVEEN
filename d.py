# dbf.py - Fixed Version with Box Deposit Logic
import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
from fpdf import FPDF
from io import BytesIO
import os
from contextlib import contextmanager

DB_FILE = "fruits.db"
BOX_DEPOSIT_DEFAULT = 200.0  # Default box deposit per box

# -------------------- Database helpers --------------------
@contextmanager
def get_conn():
    """Thread-safe database connection context manager"""
    conn = sqlite3.connect(DB_FILE, timeout=10)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_db():
    """Initialize database with all required tables"""
    with get_conn() as conn:
        c = conn.cursor()
        
        # stock / purchases (incoming)
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fruit TEXT NOT NULL,
            quantity INTEGER NOT NULL CHECK(quantity >= 0),
            cost_price REAL NOT NULL CHECK(cost_price >= 0),
            date TEXT NOT NULL,
            remaining INTEGER NOT NULL DEFAULT 0
        )""")
        
        # vendors
        c.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            contact TEXT NOT NULL
        )""")
        
        # sales
        c.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dt TEXT NOT NULL,
            vendor_id INTEGER NOT NULL,
            fruit TEXT NOT NULL,
            boxes INTEGER NOT NULL CHECK(boxes > 0),
            price_per_box REAL NOT NULL CHECK(price_per_box >= 0),
            total_price REAL NOT NULL,
            box_deposit_per_box REAL NOT NULL CHECK(box_deposit_per_box >= 0),
            box_deposit_collected REAL NOT NULL,
            note TEXT,
            FOREIGN KEY(vendor_id) REFERENCES vendors(id)
        )""")
        
        # returns
        c.execute("""
        CREATE TABLE IF NOT EXISTS returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dt TEXT NOT NULL,
            vendor_id INTEGER NOT NULL,
            fruit TEXT NOT NULL,
            boxes_returned INTEGER NOT NULL CHECK(boxes_returned > 0),
            box_deposit_refunded REAL NOT NULL CHECK(box_deposit_refunded >= 0),
            note TEXT,
            FOREIGN KEY(vendor_id) REFERENCES vendors(id)
        )""")
        
        # payments (actual payments for fruit, NOT box deposits)
        c.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dt TEXT NOT NULL,
            vendor_id INTEGER NOT NULL,
            amount REAL NOT NULL CHECK(amount > 0),
            note TEXT,
            FOREIGN KEY(vendor_id) REFERENCES vendors(id)
        )""")
        
        # rollover log
        c.execute("""
        CREATE TABLE IF NOT EXISTS rollover_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            carried INTEGER NOT NULL
        )""")
        
        # Add remaining column to existing stock table if not exists
        try:
            c.execute("SELECT remaining FROM stock LIMIT 1")
        except sqlite3.OperationalError:
            c.execute("ALTER TABLE stock ADD COLUMN remaining INTEGER DEFAULT 0")
            c.execute("UPDATE stock SET remaining = quantity WHERE remaining = 0")

def run_query(query, params=()):
    """Execute a query with error handling"""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            return True
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False

def fetch_query(query, params=()):
    """Fetch query results as DataFrame"""
    try:
        with get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database query error: {str(e)}")
        return pd.DataFrame()

# -------------------- Core operations --------------------
def add_stock(fruit, boxes, cost_per_box, dt=None):
    """Add stock with FIFO tracking"""
    if dt is None:
        dt = date.today().isoformat()
    boxes = int(boxes)
    cost_per_box = float(cost_per_box)
    
    if boxes <= 0 or cost_per_box < 0:
        st.error("Invalid stock values")
        return False
    
    return run_query(
        "INSERT INTO stock (fruit, quantity, cost_price, date, remaining) VALUES (?, ?, ?, ?, ?)",
        (fruit.upper(), boxes, cost_per_box, dt, boxes)
    )

def add_vendor(name, contact):
    """Add vendor with validation"""
    name = name.strip()
    contact = contact.strip()
    
    if not name:
        st.error("Vendor name cannot be empty")
        return False
    
    if not contact.isdigit() or len(contact) != 10:
        st.error("Contact must be exactly 10 digits")
        return False
    
    return run_query(
        "INSERT OR IGNORE INTO vendors (name, contact) VALUES (?, ?)", 
        (name, contact)
    )

@st.cache_data(ttl=60)
def list_vendors():
    """Cached vendor list"""
    return fetch_query("SELECT * FROM vendors ORDER BY name")

@st.cache_data(ttl=60)
def list_fruits():
    """Cached fruit list"""
    df = fetch_query("SELECT DISTINCT fruit FROM stock WHERE remaining > 0")
    return sorted(df['fruit'].tolist()) if not df.empty else []

@st.cache_data(ttl=30)
def get_current_stock():
    """Get current stock using FIFO remaining quantities"""
    df = fetch_query("""
        SELECT fruit, SUM(remaining) as available
        FROM stock
        GROUP BY fruit
        HAVING available > 0
        ORDER BY fruit
    """)
    
    if df.empty:
        return {}
    
    return dict(zip(df['fruit'], df['available'].astype(int)))

def reduce_stock_fifo(fruit, boxes_to_reduce):
    """Reduce stock using FIFO method"""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            
            # Get oldest stock entries with remaining quantity
            cur.execute("""
                SELECT id, remaining FROM stock 
                WHERE fruit = ? AND remaining > 0 
                ORDER BY date ASC, id ASC
            """, (fruit,))
            
            stock_entries = cur.fetchall()
            remaining_to_reduce = boxes_to_reduce
            
            for stock_id, available in stock_entries:
                if remaining_to_reduce <= 0:
                    break
                
                to_reduce = min(available, remaining_to_reduce)
                cur.execute(
                    "UPDATE stock SET remaining = remaining - ? WHERE id = ?",
                    (to_reduce, stock_id)
                )
                remaining_to_reduce -= to_reduce
            
            if remaining_to_reduce > 0:
                return False, f"Insufficient stock. Short by {remaining_to_reduce} boxes"
            
            return True, "Stock reduced successfully"
            
    except Exception as e:
        return False, f"Error reducing stock: {str(e)}"

def sell_to_vendor(dt, vendor_id, fruit, boxes, price_per_box, box_deposit_per_box, note=""):
    """Record sale with validation and FIFO stock reduction"""
    boxes = int(boxes)
    price_per_box = float(price_per_box)
    box_deposit_per_box = float(box_deposit_per_box)
    
    # Validation
    if boxes <= 0:
        st.error("Boxes must be greater than 0")
        return False
    
    # Check stock availability
    stock = get_current_stock()
    available = stock.get(fruit, 0)
    
    if boxes > available:
        st.error(f"Insufficient stock. Available: {available} boxes")
        return False
    
    # Reduce stock using FIFO
    success, message = reduce_stock_fifo(fruit, boxes)
    if not success:
        st.error(message)
        return False
    
    # Record sale
    total_price = boxes * price_per_box
    box_deposit_collected = boxes * box_deposit_per_box
    
    result = run_query("""
        INSERT INTO sales (dt, vendor_id, fruit, boxes, price_per_box, total_price, 
                          box_deposit_per_box, box_deposit_collected, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (dt, int(vendor_id), fruit, boxes, price_per_box, total_price, 
          box_deposit_per_box, box_deposit_collected, note))
    
    if result:
        st.cache_data.clear()  # Clear cache to reflect changes
    
    return result

def record_return(dt, vendor_id, fruit, boxes_returned, box_deposit_per_box, note=""):
    """Record return with box deposit refund"""
    boxes_returned = int(boxes_returned)
    box_deposit_per_box = float(box_deposit_per_box)
    
    if boxes_returned <= 0:
        st.error("Boxes returned must be greater than 0")
        return False
    
    box_deposit_refunded = box_deposit_per_box * boxes_returned
    
    # Add returned boxes back to stock and record return
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            
            # Record return
            cur.execute("""
                INSERT INTO returns (dt, vendor_id, fruit, boxes_returned, box_deposit_refunded, note)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (dt, int(vendor_id), fruit, boxes_returned, box_deposit_refunded, note))
            
            # Add back to stock with today's date
            cur.execute("""
                INSERT INTO stock (fruit, quantity, cost_price, date, remaining)
                SELECT ?, ?, AVG(cost_price), ?, ?
                FROM stock WHERE fruit = ?
            """, (fruit, boxes_returned, dt, boxes_returned, fruit))
            
        st.cache_data.clear()
        return True
        
    except Exception as e:
        st.error(f"Error recording return: {str(e)}")
        return False

def record_payment(dt, vendor_id, amount, note=""):
    """Record payment (for fruit purchase, NOT box deposit)"""
    amount = float(amount)
    
    if amount <= 0:
        st.error("Payment amount must be greater than 0")
        return False
    
    result = run_query(
        "INSERT INTO payments (dt, vendor_id, amount, note) VALUES (?, ?, ?, ?)",
        (dt, int(vendor_id), amount, note)
    )
    
    if result:
        st.cache_data.clear()
    
    return result

# -------------------- Reporting helpers --------------------
def compute_weighted_avg_cost(fruit, up_to_date=None):
    """Compute weighted average cost per box"""
    if up_to_date is None:
        query = "SELECT quantity, cost_price FROM stock WHERE fruit = ?"
        params = (fruit,)
    else:
        query = "SELECT quantity, cost_price FROM stock WHERE fruit = ? AND date <= ?"
        params = (fruit, up_to_date)
    
    df = fetch_query(query, params)
    
    if df.empty:
        return 0.0
    
    total_boxes = df['quantity'].sum()
    total_cost = (df['quantity'] * df['cost_price']).sum()
    
    if total_boxes <= 0:
        return 0.0
    
    return float(total_cost / total_boxes)

def compute_cogs_for_sales(sales_df, up_to_date=None):
    """Compute COGS using weighted average cost"""
    if sales_df.empty:
        return 0.0
    
    cogs = 0.0
    for fruit, grp in sales_df.groupby('fruit'):
        sold_boxes = int(grp['boxes'].sum())
        avg_cost = compute_weighted_avg_cost(fruit, up_to_date)
        cogs += avg_cost * sold_boxes
    
    return cogs

def vendor_summary_table():
    """Generate vendor summary with dues (excluding box deposits from payment calculation)"""
    vendors = fetch_query("SELECT id, name FROM vendors")
    
    if vendors.empty:
        return pd.DataFrame()
    
    rows = []
    
    for _, v in vendors.iterrows():
        vid = v['id']
        vname = v['name']
        
        # Get sales data
        sales_df = fetch_query("""
            SELECT COALESCE(SUM(total_price), 0) as total_sales, 
                   COALESCE(SUM(box_deposit_collected), 0) as deposits_collected 
            FROM sales WHERE vendor_id = ?
        """, (vid,))
        
        total_sales = float(sales_df['total_sales'].iloc[0]) if not sales_df.empty else 0.0
        deposits_collected = float(sales_df['deposits_collected'].iloc[0]) if not sales_df.empty else 0.0
        
        # Get box deposit refunds
        refunds_df = fetch_query("""
            SELECT COALESCE(SUM(box_deposit_refunded), 0) as deposits_refunded 
            FROM returns WHERE vendor_id = ?
        """, (vid,))
        
        deposits_refunded = float(refunds_df['deposits_refunded'].iloc[0]) if not refunds_df.empty else 0.0
        
        # Get actual payments (for fruit, not deposits)
        payments_df = fetch_query("""
            SELECT COALESCE(SUM(amount), 0) as paid 
            FROM payments WHERE vendor_id = ?
        """, (vid,))
        
        paid = float(payments_df['paid'].iloc[0]) if not payments_df.empty else 0.0
        
        # Net box deposits held = deposits_collected - deposits_refunded
        net_box_deposits_held = deposits_collected - deposits_refunded
        
        # Net due for fruit = total_sales - payments (box deposits are separate)
        net_due = total_sales - paid
        
        rows.append({
            "vendor_id": vid,
            "vendor_name": vname,
            "total_sales": total_sales,
            "payments": paid,
            "net_due": net_due,
            "deposits_collected": deposits_collected,
            "deposits_refunded": deposits_refunded,
            "net_deposits_held": net_box_deposits_held
        })
    
    return pd.DataFrame(rows)

def vendor_ledger_df(vendor_id):
    """Generate vendor ledger with running balance (separating fruit payments and box deposits)"""
    # Get all transactions
    sales = fetch_query("""
        SELECT dt as date, 'SALE' as type, fruit, boxes as qty, 
               total_price as sale_amount, box_deposit_collected as deposit, note
        FROM sales WHERE vendor_id = ?
    """, (vendor_id,))
    
    payments = fetch_query("""
        SELECT dt as date, 'PAYMENT' as type, NULL as fruit, NULL as qty, 
               -amount as sale_amount, 0 as deposit, note
        FROM payments WHERE vendor_id = ?
    """, (vendor_id,))
    
    returns = fetch_query("""
        SELECT dt as date, 'RETURN' as type, fruit, -boxes_returned as qty, 
               0 as sale_amount, -box_deposit_refunded as deposit, note
        FROM returns WHERE vendor_id = ?
    """, (vendor_id,))
    
    df = pd.concat([sales, payments, returns], sort=False, ignore_index=True)
    
    if df.empty:
        return df
    
    df = df.sort_values("date", ignore_index=True)
    df['sale_amount'] = df['sale_amount'].fillna(0).astype(float)
    df['deposit'] = df['deposit'].fillna(0).astype(float)
    
    # Calculate running balances
    df['running_due'] = df['sale_amount'].cumsum()  # Money owed for fruit
    df['running_deposits'] = df['deposit'].cumsum()  # Net box deposits held
    
    return df

# -------------------- Daily rollover --------------------
def carry_forward_stock():
    """Carry forward stock for new day"""
    today_str = date.today().isoformat()
    
    # Check if already done today
    existing = fetch_query("SELECT * FROM rollover_log WHERE date = ?", (today_str,))
    if not existing.empty:
        return False
    
    # Log the rollover
    run_query("INSERT INTO rollover_log (date, carried) VALUES (?, ?)", (today_str, 1))
    
    return True

# -------------------- Export functions --------------------
def export_to_excel(df):
    """Export DataFrame to Excel"""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Report")
    buf.seek(0)
    return buf

def export_to_pdf(df, title="Report"):
    """Export DataFrame to PDF with proper font handling"""
    pdf = FPDF(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=10)
    
    # Use only Helvetica font (always available)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, title, ln=1, align='C')
    pdf.ln(4)
    
    # Sanitize text
    def safe_text(x):
        if pd.isna(x):
            return ""
        text = str(x).replace("₹", "Rs.")
        return text.encode('latin-1', 'replace').decode('latin-1')
    
    cols = list(df.columns)
    page_width = pdf.w - 2 * pdf.l_margin
    col_w = max(20, page_width / max(1, len(cols)))
    
    pdf.set_font("Helvetica", "B", 9)
    
    # Header
    for col in cols:
        pdf.cell(col_w, 8, safe_text(col), border=1)
    pdf.ln()
    
    # Rows
    pdf.set_font("Helvetica", "", 8)
    for _, row in df.iterrows():
        for item in row:
            text = safe_text(item)
            if len(text) > 40:
                text = text[:37] + "..."
            pdf.cell(col_w, 7, text, border=1)
        pdf.ln()
        
        if pdf.get_y() > pdf.h - 20:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 9)
            for col in cols:
                pdf.cell(col_w, 8, safe_text(col), border=1)
            pdf.ln()
            pdf.set_font("Helvetica", "", 8)
    
    pdf_output = pdf.output(dest="S")
    if isinstance(pdf_output, str):
        pdf_output = pdf_output.encode('latin-1', 'replace')
    
    buf = BytesIO(pdf_output)
    buf.seek(0)
    return buf

# -------------------- Streamlit App UI --------------------
st.set_page_config(page_title="DBF Fruit Manager", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🍎 DBF Fruit Manager — Box Deposits & Vendor Payments")

# Initialize database
init_db()

# Check if rollover needed
today_str = date.today().isoformat()
rollover_df = fetch_query("SELECT * FROM rollover_log WHERE date = ?", (today_str,))
if rollover_df.empty:
    if st.button("🔄 Start New Day", type="primary"):
        if carry_forward_stock():
            st.success("✅ New day started!")
            st.rerun()

tabs = st.tabs([
    "📋 Vendors", 
    "📦 Morning Stock", 
    "💰 Sell", 
    "↩️ Returns", 
    "💵 Payments", 
    "📊 Vendor Dues", 
    "📈 Reports", 
    "📖 Ledger"
])

# ---------- Tab 0: Vendors ----------
with tabs[0]:
    st.header("Vendors Management")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Add New Vendor")
        with st.form("vendor_form", clear_on_submit=True):
            vname = st.text_input("Vendor Name *", placeholder="Enter vendor name")
            vcontact = st.text_input("Contact Number *", placeholder="10 digits", max_chars=10)
            
            if st.form_submit_button("➕ Add Vendor", type="primary"):
                if add_vendor(vname, vcontact):
                    st.success(f"✅ Vendor '{vname}' added successfully!")
                    st.cache_data.clear()
                    st.rerun()
    
    with col2:
        st.subheader("All Vendors")
        vendors_df = list_vendors()
        if vendors_df.empty:
            st.info("No vendors yet. Add your first vendor!")
        else:
            st.dataframe(vendors_df, use_container_width=True, hide_index=True)

# ---------- Tab 1: Morning Stock ----------
with tabs[1]:
    st.header("Morning Stock Entry")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Add Incoming Stock")
        with st.form("stock_form", clear_on_submit=True):
            fruit = st.text_input("Fruit Name *", value="", placeholder="e.g., APPLE").upper()
            boxes = st.number_input("Boxes Received *", min_value=1, value=10, step=1)
            cost = st.number_input("Cost per Box (₹) *", min_value=0.0, value=500.0, step=10.0)
            stock_date = st.date_input("Date", value=date.today())
            
            if st.form_submit_button("📦 Add Stock", type="primary"):
                if fruit.strip():
                    if add_stock(fruit.strip(), boxes, cost, stock_date.isoformat()):
                        st.success(f"✅ Added {boxes} boxes of {fruit} at ₹{cost}/box")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.error("Fruit name is required")
    
    with col2:
        st.subheader("Current Stock Available")
        stock = get_current_stock()
        if stock:
            stock_df = pd.DataFrame(list(stock.items()), columns=['Fruit', 'Boxes Available'])
            st.dataframe(stock_df, use_container_width=True, hide_index=True)
        else:
            st.info("No stock available. Add stock to get started!")

# ---------- Tab 2: Sell ----------
with tabs[2]:
    st.header("Record Sale")
    
    st.info("💡 Box deposit is a refundable security deposit for plastic boxes, separate from fruit payment")
    
    vendors_df = list_vendors()
    fruits = list_fruits()
    
    if vendors_df.empty:
        st.warning("⚠️ Please add vendors first in the Vendors tab")
    elif not fruits:
        st.warning("⚠️ Please add stock first in the Morning Stock tab")
    else:
        with st.form("sell_form", clear_on_submit=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                sdate = st.date_input("Sale Date *", value=date.today())
                vendor_choice = st.selectbox("Vendor *", vendors_df['name'].tolist())
                vendor_id = int(vendors_df[vendors_df['name'] == vendor_choice]['id'].iloc[0])
            
            with col2:
                fruit_choice = st.selectbox("Fruit *", fruits)
                sell_boxes = st.number_input("Boxes to Sell *", min_value=1, value=1, step=1)
                current_stock = get_current_stock()
                st.caption(f"Available: {current_stock.get(fruit_choice, 0)} boxes")
            
            with col3:
                price_box = st.number_input("Price per Box (₹) *", min_value=0.0, value=700.0, step=10.0)
                box_deposit = st.number_input("Box Deposit per Box (₹) *", min_value=0.0, value=BOX_DEPOSIT_DEFAULT, step=10.0, 
                                             help="Refundable security deposit for the plastic box")
                st.caption(f"Sale Total: ₹{sell_boxes * price_box:.2f}")
                st.caption(f"Box Deposit: ₹{sell_boxes * box_deposit:.2f}")
            
            note = st.text_area("Note (optional)", placeholder="Any additional notes...")
            
            if st.form_submit_button("💰 Record Sale", type="primary"):
                if sell_to_vendor(sdate.isoformat(), vendor_id, fruit_choice, sell_boxes, 
                                 price_box, box_deposit, note):
                    st.success(f"✅ Sale recorded: {sell_boxes} boxes of {fruit_choice} to {vendor_choice}")
                    st.info(f"📦 Box deposit collected: ₹{sell_boxes * box_deposit:.2f} (refundable on return)")
                    st.balloons()
                    st.rerun()

# ---------- Tab 3: Returns ----------
with tabs[3]:
    st.header("Record Box Returns")
    
    st.info("💡 When boxes are returned, the box deposit is refunded. The fruit payment remains due.")
    
    vendors_df = list_vendors()
    
    if vendors_df.empty:
        st.warning("⚠️ Please add vendors first")
    else:
        with st.form("return_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                rdate = st.date_input("Return Date *", value=date.today())
                v_choice = st.selectbox("Vendor *", vendors_df['name'].tolist())
                v_id = int(vendors_df[vendors_df['name'] == v_choice]['id'].iloc[0])
            
            with col2:
                vendor_fruits = fetch_query("""
                    SELECT DISTINCT fruit FROM sales WHERE vendor_id = ? ORDER BY fruit
                """, (v_id,))
                
                if vendor_fruits.empty:
                    st.warning(f"No sales recorded for {v_choice} yet")
                    fruit_r = st.text_input("Fruit Name", value="APPLE")
                else:
                    fruit_r = st.selectbox("Fruit *", vendor_fruits['fruit'].tolist())
                
                returned_boxes = st.number_input("Boxes Returned *", min_value=1, value=1, step=1)
            
            box_deposit = st.number_input("Box Deposit per Box (₹) *", min_value=0.0, value=BOX_DEPOSIT_DEFAULT, step=10.0)
            st.caption(f"Deposit Refund: ₹{returned_boxes * box_deposit:.2f}")
            
            rnote = st.text_area("Note (optional)", placeholder="Reason for return...")
            
            if st.form_submit_button("↩️ Record Return", type="primary"):
                if record_return(rdate.isoformat(), v_id, fruit_r, returned_boxes, box_deposit, rnote):
                    st.success(f"✅ Return recorded: {returned_boxes} boxes from {v_choice}")
                    st.info(f"💰 Box deposit refunded: ₹{returned_boxes * box_deposit:.2f}")
                    st.rerun()

# ---------- Tab 4: Vendor Payments ----------
with tabs[4]:
    st.header("Record Fruit Payments")
    
    st.info("💡 Record actual payments received for the fruit sold (NOT box deposits)")
    
    vendors_df = list_vendors()
    
    if vendors_df.empty:
        st.warning("⚠️ Please add vendors first")
    else:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("New Payment")
            with st.form("payment_form", clear_on_submit=True):
                pdate = st.date_input("Payment Date *", value=date.today())
                vpay = st.selectbox("Vendor *", vendors_df['name'].tolist())
                vid = int(vendors_df[vendors_df['name'] == vpay]['id'].iloc[0])
                
                # Show vendor's current due
                summary = vendor_summary_table()
                if not summary.empty:
                    vendor_due = summary[summary['vendor_id'] == vid]['net_due'].iloc[0]
                    deposits_held = summary[summary['vendor_id'] == vid]['net_deposits_held'].iloc[0]
                    st.metric("Amount Due (for fruit)", f"₹{vendor_due:.2f}")
                    st.metric("Box Deposits Held", f"₹{deposits_held:.2f}", help="Security deposit for boxes")
                
                amount = st.number_input("Payment Amount (₹) *", min_value=0.0, value=0.0, step=100.0,
                                        help="Payment for fruit purchase only")
                pnote = st.text_area("Note (optional)", placeholder="Payment details...")
                
                if st.form_submit_button("💵 Record Payment", type="primary"):
                    if record_payment(pdate.isoformat(), vid, amount, pnote):
                        st.success(f"✅ Payment of ₹{amount} recorded from {vpay}")
                        st.rerun()
        
        with col2:
            st.subheader("Recent Payments")
            recent_p = fetch_query("""
                SELECT p.dt as Date, v.name as Vendor, p.amount as Amount, p.note as Note 
                FROM payments p 
                LEFT JOIN vendors v ON p.vendor_id = v.id 
                ORDER BY p.dt DESC 
                LIMIT 50
            """)
            if not recent_p.empty:
                recent_p['Amount'] = recent_p['Amount'].apply(lambda x: f"₹{x:.2f}")
                st.dataframe(recent_p, use_container_width=True, hide_index=True)
            else:
                st.info("No payments recorded yet")

# ---------- Tab 5: Vendor Dues ----------
with tabs[5]:
    st.header("Vendor Dues Summary")
    
    st.info("💡 Amount Due = Total Sales - Payments (Box deposits tracked separately)")
    
    summary = vendor_summary_table()
    
    if summary.empty:
        st.info("No transactions yet")
    else:
        # Display metrics
        total_sales = summary['total_sales'].sum()
        total_due = summary['net_due'].sum()
        total_paid = summary['payments'].sum()
        total_deposits = summary['net_deposits_held'].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sales", f"₹{total_sales:.2f}")
        col2.metric("Total Paid", f"₹{total_paid:.2f}")
        col3.metric("Total Due", f"₹{total_due:.2f}")
        col4.metric("Box Deposits Held", f"₹{total_deposits:.2f}", help="Security deposits (refundable)")
        
        st.divider()
        
        # Format and display summary
        summary_display = summary[['vendor_name','total_sales','payments','net_due','deposits_collected','deposits_refunded','net_deposits_held']].copy()
        summary_display.columns = ['Vendor', 'Total Sales (₹)', 'Payments (₹)', 'Amount Due (₹)', 
                                   'Deposits Collected (₹)', 'Deposits Refunded (₹)', 'Net Deposits Held (₹)']
        
        # Format currency
        for col in ['Total Sales (₹)', 'Payments (₹)', 'Amount Due (₹)', 'Deposits Collected (₹)', 
                    'Deposits Refunded (₹)', 'Net Deposits Held (₹)']:
            summary_display[col] = summary_display[col].apply(lambda x: f"₹{x:.2f}")
        
        st.dataframe(summary_display, use_container_width=True, hide_index=True)
        
        # Export options
        st.divider()
        col1, col2 = st.columns(2)
        
        with col1:
            excel_buf = export_to_excel(summary)
            st.download_button(
                "📥 Download Excel",
                data=excel_buf,
                file_name=f"Vendor_Dues_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col2:
            pdf_buf = export_to_pdf(summary, title="Vendor Dues Report")
            st.download_button(
                "📥 Download PDF",
                data=pdf_buf,
                file_name=f"Vendor_Dues_{date.today()}.pdf",
                mime="application/pdf"
            )

# ---------- Tab 6: Reports ----------
with tabs[6]:
    st.header("Sales & Profit/Loss Reports")
    
    # Date filter
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", value=date.today().replace(day=1))
    with col2:
        end_date = st.date_input("To Date", value=date.today())
    
    # Fetch sales data
    sales_df = fetch_query("""
        SELECT s.dt as Date, v.name as Vendor, s.fruit as Fruit, 
               s.boxes as Boxes, s.price_per_box as Price, 
               s.total_price as Total, s.box_deposit_collected as Box_Deposit
        FROM sales s 
        LEFT JOIN vendors v ON s.vendor_id = v.id
        WHERE s.dt BETWEEN ? AND ?
        ORDER BY s.dt DESC
    """, (start_date.isoformat(), end_date.isoformat()))
    
    if sales_df.empty:
        st.info("No sales in selected date range")
    else:
        # Calculate metrics
        total_revenue = float(sales_df['Total'].sum())
        cogs = compute_cogs_for_sales(sales_df.rename(columns={'Boxes':'boxes','Fruit':'fruit'}))
        pnl = total_revenue - cogs
        total_deposits = float(sales_df['Box_Deposit'].sum())
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Revenue", f"₹{total_revenue:.2f}")
        col2.metric("COGS", f"₹{cogs:.2f}")
        col3.metric("P&L", f"₹{pnl:.2f}", delta=f"{(pnl/cogs)*100:.1f}% margin" if cogs > 0 else "")
        col4.metric("Box Deposits", f"₹{total_deposits:.2f}", help="Security deposits collected")
        
        st.divider()
        
        # Format sales data
        sales_display = sales_df.copy()
        sales_display['Price'] = sales_display['Price'].apply(lambda x: f"₹{x:.2f}")
        sales_display['Total'] = sales_display['Total'].apply(lambda x: f"₹{x:.2f}")
        sales_display['Box_Deposit'] = sales_display['Box_Deposit'].apply(lambda x: f"₹{x:.2f}")
        sales_display.columns = ['Date', 'Vendor', 'Fruit', 'Boxes', 'Price/Box', 'Total', 'Box Deposit']
        
        st.subheader("Sales Details")
        st.dataframe(sales_display, use_container_width=True, hide_index=True)
        
        # Export options
        st.divider()
        col1, col2 = st.columns(2)
        
        with col1:
            excel_buf = export_to_excel(sales_df)
            st.download_button(
                "📥 Download Excel",
                data=excel_buf,
                file_name=f"Sales_Report_{start_date}_{end_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col2:
            pdf_buf = export_to_pdf(sales_df, title=f"Sales Report ({start_date} to {end_date})")
            st.download_button(
                "📥 Download PDF",
                data=pdf_buf,
                file_name=f"Sales_Report_{start_date}_{end_date}.pdf",
                mime="application/pdf"
            )

# ---------- Tab 7: Vendor Ledger ----------
with tabs[7]:
    st.header("Vendor Ledger")
    
    st.info("💡 Running Due = Amount owed for fruit | Running Deposits = Box deposits held (separate)")
    
    vendors_df = list_vendors()
    
    if vendors_df.empty:
        st.warning("⚠️ Please add vendors first")
    else:
        vchoice = st.selectbox("Select Vendor", vendors_df['name'].tolist())
        vid = int(vendors_df[vendors_df['name'] == vchoice]['id'].iloc[0])
        
        ledger = vendor_ledger_df(vid)
        
        if ledger.empty:
            st.info(f"No transactions for {vchoice} yet")
        else:
            # Show current balances
            final_due = ledger['running_due'].iloc[-1]
            final_deposits = ledger['running_deposits'].iloc[-1]
            
            col1, col2 = st.columns(2)
            col1.metric("Amount Due (Fruit)", f"₹{final_due:.2f}")
            col2.metric("Box Deposits Held", f"₹{final_deposits:.2f}", help="Refundable security deposit")
            
            st.divider()
            
            # Format ledger
            ledger_display = ledger.copy()
            ledger_display['sale_amount'] = ledger_display['sale_amount'].apply(lambda x: f"₹{x:.2f}")
            ledger_display['deposit'] = ledger_display['deposit'].apply(lambda x: f"₹{x:.2f}")
            ledger_display['running_due'] = ledger_display['running_due'].apply(lambda x: f"₹{x:.2f}")
            ledger_display['running_deposits'] = ledger_display['running_deposits'].apply(lambda x: f"₹{x:.2f}")
            
            ledger_display.columns = ['Date', 'Type', 'Fruit', 'Qty', 'Sale/Payment', 'Box Deposit', 
                                     'Note', 'Running Due', 'Running Deposits']
            
            st.dataframe(ledger_display, use_container_width=True, hide_index=True)
            
            # Export options
            st.divider()
            col1, col2 = st.columns(2)
            
            with col1:
                excel_buf = export_to_excel(ledger)
                st.download_button(
                    "📥 Download Excel",
                    data=excel_buf,
                    file_name=f"{vchoice}_Ledger_{date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            with col2:
                pdf_buf = export_to_pdf(ledger, title=f"{vchoice} - Vendor Ledger")
                st.download_button(
                    "📥 Download PDF",
                    data=pdf_buf,
                    file_name=f"{vchoice}_Ledger_{date.today()}.pdf",
                    mime="application/pdf"
                )

# Footer
st.divider()
st.caption("🍎 DBF Fruit Manager v2.1 - Box Deposit System")
