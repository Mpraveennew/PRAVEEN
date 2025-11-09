# dbf.py - Supabase Version with Persistent Storage
import streamlit as st
import pandas as pd
from datetime import date
from fpdf import FPDF
from io import BytesIO
from st_supabase_connection import SupabaseConnection

BOX_DEPOSIT_DEFAULT = 200.0

# -------------------- Supabase Connection --------------------
@st.cache_resource
def init_connection():
    """Initialize Supabase connection"""
    return st.connection("supabase", type=SupabaseConnection)

supabase = init_connection()

# -------------------- Database helpers --------------------
def execute_query(table, operation, data=None, filters=None):
    """Generic query executor with error handling"""
    try:
        query = supabase.table(table)
        
        if operation == "insert":
            result = query.insert(data).execute()
        elif operation == "select":
            query = query.select("*")
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            result = query.execute()
        elif operation == "update":
            query = query.update(data)
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            result = query.execute()
        elif operation == "delete":
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            result = query.delete().execute()
        
        return True, result
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False, None

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
    
    data = {
        "fruit": fruit.upper(),
        "quantity": boxes,
        "cost_price": cost_per_box,
        "date": dt,
        "remaining": boxes
    }
    
    success, _ = execute_query("stock", "insert", data)
    if success:
        st.cache_data.clear()
    return success

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
    
    data = {"name": name, "contact": contact}
    success, _ = execute_query("vendors", "insert", data)
    
    if success:
        st.cache_data.clear()
    return success

@st.cache_data(ttl=60)
def list_vendors():
    """Cached vendor list"""
    try:
        response = supabase.table("vendors").select("*").order("name").execute()
        return pd.DataFrame(response.data)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def list_fruits():
    """Cached fruit list with available stock"""
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        df = pd.DataFrame(response.data)
        if df.empty:
            return []
        df = df[df['remaining'] > 0]
        return sorted(df['fruit'].unique().tolist())
    except:
        return []

@st.cache_data(ttl=30)
def get_current_stock():
    """Get current stock aggregated by fruit"""
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        df = pd.DataFrame(response.data)
        if df.empty:
            return {}
        stock_summary = df.groupby('fruit')['remaining'].sum()
        return {k: int(v) for k, v in stock_summary.items() if v > 0}
    except:
        return {}

def reduce_stock_fifo(fruit, boxes_to_reduce):
    """Reduce stock using FIFO method"""
    try:
        # Get stock entries ordered by date
        response = supabase.table("stock")\
            .select("*")\
            .eq("fruit", fruit)\
            .gt("remaining", 0)\
            .order("date")\
            .order("id")\
            .execute()
        
        stock_entries = response.data
        remaining_to_reduce = boxes_to_reduce
        
        for entry in stock_entries:
            if remaining_to_reduce <= 0:
                break
            
            available = entry['remaining']
            to_reduce = min(available, remaining_to_reduce)
            
            # Update remaining quantity
            new_remaining = available - to_reduce
            supabase.table("stock")\
                .update({"remaining": new_remaining})\
                .eq("id", entry['id'])\
                .execute()
            
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
    
    if boxes <= 0:
        st.error("Boxes must be greater than 0")
        return False
    
    # Check stock
    stock = get_current_stock()
    available = stock.get(fruit, 0)
    
    if boxes > available:
        st.error(f"Insufficient stock. Available: {available} boxes")
        return False
    
    # Reduce stock
    success, message = reduce_stock_fifo(fruit, boxes)
    if not success:
        st.error(message)
        return False
    
    # Record sale
    total_price = boxes * price_per_box
    box_deposit_collected = boxes * box_deposit_per_box
    
    sale_data = {
        "dt": dt,
        "vendor_id": int(vendor_id),
        "fruit": fruit,
        "boxes": boxes,
        "price_per_box": price_per_box,
        "total_price": total_price,
        "box_deposit_per_box": box_deposit_per_box,
        "box_deposit_collected": box_deposit_collected,
        "note": note
    }
    
    success, _ = execute_query("sales", "insert", sale_data)
    
    if success:
        st.cache_data.clear()
    
    return success

def record_return(dt, vendor_id, fruit, boxes_returned, box_deposit_per_box, note=""):
    """Record return with box deposit refund"""
    boxes_returned = int(boxes_returned)
    box_deposit_per_box = float(box_deposit_per_box)
    
    if boxes_returned <= 0:
        st.error("Boxes returned must be greater than 0")
        return False
    
    box_deposit_refunded = box_deposit_per_box * boxes_returned
    
    try:
        # Record return
        return_data = {
            "dt": dt,
            "vendor_id": int(vendor_id),
            "fruit": fruit,
            "boxes_returned": boxes_returned,
            "box_deposit_refunded": box_deposit_refunded,
            "note": note
        }
        
        supabase.table("returns").insert(return_data).execute()
        
        # Get average cost for returned fruit
        cost_response = supabase.table("stock")\
            .select("cost_price, quantity")\
            .eq("fruit", fruit)\
            .execute()
        
        if cost_response.data:
            df = pd.DataFrame(cost_response.data)
            avg_cost = (df['cost_price'] * df['quantity']).sum() / df['quantity'].sum()
        else:
            avg_cost = 0
        
        # Add back to stock
        stock_data = {
            "fruit": fruit,
            "quantity": boxes_returned,
            "cost_price": avg_cost,
            "date": dt,
            "remaining": boxes_returned
        }
        
        supabase.table("stock").insert(stock_data).execute()
        
        st.cache_data.clear()
        return True
        
    except Exception as e:
        st.error(f"Error recording return: {str(e)}")
        return False

def record_payment(dt, vendor_id, amount, note=""):
    """Record payment for fruit purchase"""
    amount = float(amount)
    
    if amount <= 0:
        st.error("Payment amount must be greater than 0")
        return False
    
    payment_data = {
        "dt": dt,
        "vendor_id": int(vendor_id),
        "amount": amount,
        "note": note
    }
    
    success, _ = execute_query("payments", "insert", payment_data)
    
    if success:
        st.cache_data.clear()
    
    return success

# -------------------- Reporting helpers --------------------
def compute_weighted_avg_cost(fruit, up_to_date=None):
    """Compute weighted average cost per box"""
    try:
        query = supabase.table("stock").select("quantity, cost_price").eq("fruit", fruit)
        
        if up_to_date:
            query = query.lte("date", up_to_date)
        
        response = query.execute()
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return 0.0
        
        total_boxes = df['quantity'].sum()
        total_cost = (df['quantity'] * df['cost_price']).sum()
        
        if total_boxes <= 0:
            return 0.0
        
        return float(total_cost / total_boxes)
    except:
        return 0.0

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
    """Generate vendor summary with dues"""
    try:
        vendors_response = supabase.table("vendors").select("id, name").execute()
        vendors = pd.DataFrame(vendors_response.data)
        
        if vendors.empty:
            return pd.DataFrame()
        
        rows = []
        
        for _, v in vendors.iterrows():
            vid = v['id']
            vname = v['name']
            
            # Get sales
            sales_response = supabase.table("sales")\
                .select("total_price, box_deposit_collected")\
                .eq("vendor_id", vid)\
                .execute()
            sales_df = pd.DataFrame(sales_response.data)
            
            total_sales = float(sales_df['total_price'].sum()) if not sales_df.empty else 0.0
            deposits_collected = float(sales_df['box_deposit_collected'].sum()) if not sales_df.empty else 0.0
            
            # Get returns
            returns_response = supabase.table("returns")\
                .select("box_deposit_refunded")\
                .eq("vendor_id", vid)\
                .execute()
            returns_df = pd.DataFrame(returns_response.data)
            
            deposits_refunded = float(returns_df['box_deposit_refunded'].sum()) if not returns_df.empty else 0.0
            
            # Get payments
            payments_response = supabase.table("payments")\
                .select("amount")\
                .eq("vendor_id", vid)\
                .execute()
            payments_df = pd.DataFrame(payments_response.data)
            
            paid = float(payments_df['amount'].sum()) if not payments_df.empty else 0.0
            
            net_box_deposits_held = deposits_collected - deposits_refunded
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
    except:
        return pd.DataFrame()

def vendor_ledger_df(vendor_id):
    """Generate vendor ledger with running balance"""
    try:
        # Get sales
        sales_response = supabase.table("sales")\
            .select("dt, fruit, boxes, total_price, box_deposit_collected, note")\
            .eq("vendor_id", vendor_id)\
            .execute()
        sales = pd.DataFrame(sales_response.data)
        
        if not sales.empty:
            sales['type'] = 'SALE'
            sales['qty'] = sales['boxes']
            sales['sale_amount'] = sales['total_price']
            sales['deposit'] = sales['box_deposit_collected']
            sales = sales.rename(columns={'dt': 'date'})
            sales = sales[['date', 'type', 'fruit', 'qty', 'sale_amount', 'deposit', 'note']]
        
        # Get payments
        payments_response = supabase.table("payments")\
            .select("dt, amount, note")\
            .eq("vendor_id", vendor_id)\
            .execute()
        payments = pd.DataFrame(payments_response.data)
        
        if not payments.empty:
            payments['type'] = 'PAYMENT'
            payments['fruit'] = None
            payments['qty'] = None
            payments['sale_amount'] = -payments['amount']
            payments['deposit'] = 0
            payments = payments.rename(columns={'dt': 'date'})
            payments = payments[['date', 'type', 'fruit', 'qty', 'sale_amount', 'deposit', 'note']]
        
        # Get returns
        returns_response = supabase.table("returns")\
            .select("dt, fruit, boxes_returned, box_deposit_refunded, note")\
            .eq("vendor_id", vendor_id)\
            .execute()
        returns = pd.DataFrame(returns_response.data)
        
        if not returns.empty:
            returns['type'] = 'RETURN'
            returns['qty'] = -returns['boxes_returned']
            returns['sale_amount'] = 0
            returns['deposit'] = -returns['box_deposit_refunded']
            returns = returns.rename(columns={'dt': 'date'})
            returns = returns[['date', 'type', 'fruit', 'qty', 'sale_amount', 'deposit', 'note']]
        
        # Combine all
        dfs = [df for df in [sales, payments, returns] if not df.empty]
        
        if not dfs:
            return pd.DataFrame()
        
        df = pd.concat(dfs, ignore_index=True)
        df = df.sort_values("date", ignore_index=True)
        
        df['sale_amount'] = df['sale_amount'].fillna(0).astype(float)
        df['deposit'] = df['deposit'].fillna(0).astype(float)
        
        df['running_due'] = df['sale_amount'].cumsum()
        df['running_deposits'] = df['deposit'].cumsum()
        
        return df
    except:
        return pd.DataFrame()

# -------------------- Daily rollover --------------------
def carry_forward_stock():
    """Carry forward stock for new day"""
    today_str = date.today().isoformat()
    
    try:
        # Check if already done
        response = supabase.table("rollover_log")\
            .select("*")\
            .eq("date", today_str)\
            .execute()
        
        if response.data:
            return False
        
        # Log rollover
        supabase.table("rollover_log").insert({
            "date": today_str,
            "carried": 1
        }).execute()
        
        return True
    except:
        return False

# -------------------- Export functions --------------------
def export_to_excel(df):
    """Export DataFrame to Excel"""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Report")
    buf.seek(0)
    return buf

def export_to_pdf(df, title="Report"):
    """Export DataFrame to PDF"""
    pdf = FPDF(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=10)
    
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, title, ln=1, align='C')
    pdf.ln(4)
    
    def safe_text(x):
        if pd.isna(x):
            return ""
        text = str(x).replace("₹", "Rs.")
        return text.encode('latin-1', 'replace').decode('latin-1')
    
    cols = list(df.columns)
    page_width = pdf.w - 2 * pdf.l_margin
    col_w = max(20, page_width / max(1, len(cols)))
    
    pdf.set_font("Helvetica", "B", 9)
    
    for col in cols:
        pdf.cell(col_w, 8, safe_text(col), border=1)
    pdf.ln()
    
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

st.title("🍎 DBF Fruit Manager — Cloud Persistent Storage")

# Check rollover
today_str = date.today().isoformat()
try:
    rollover_response = supabase.table("rollover_log").select("*").eq("date", today_str).execute()
    if not rollover_response.data:
        if st.button("🔄 Start New Day", type="primary"):
            if carry_forward_stock():
                st.success("✅ New day started!")
                st.rerun()
except:
    pass

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
                box_deposit = st.number_input("Box Deposit per Box (₹) *", min_value=0.0, value=BOX_DEPOSIT_DEFAULT, step=10.0)
                st.caption(f"Sale Total: ₹{sell_boxes * price_box:.2f}")
                st.caption(f"Box Deposit: ₹{sell_boxes * box_deposit:.2f}")
            
            note = st.text_area("Note (optional)", placeholder="Any additional notes...")
            
            if st.form_submit_button("💰 Record Sale", type="primary"):
                if sell_to_vendor(sdate.isoformat(), vendor_id, fruit_choice, sell_boxes, 
                                 price_box, box_deposit, note):
                    st.success(f"✅ Sale recorded: {sell_boxes} boxes of {fruit_choice} to {vendor_choice}")
                    st.info(f"📦 Box deposit collected: ₹{sell_boxes * box_deposit:.2f}")
                    st.balloons()
                    st.rerun()

# ---------- Tab 3: Returns ----------
with tabs[3]:
    st.header("Record Box Returns")
    
    st.info("💡 When boxes are returned, the box deposit is refunded")
    
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
                # Get vendor's fruits
                try:
                    vendor_fruits_response = supabase.table("sales")\
                        .select("fruit")\
                        .eq("vendor_id", v_id)\
                        .execute()
                    vendor_fruits_data = pd.DataFrame(vendor_fruits_response.data)
                    
                    if vendor_fruits_data.empty:
                        st.warning(f"No sales recorded for {v_choice} yet")
                        fruit_r = st.text_input("Fruit Name", value="APPLE")
                    else:
                        unique_fruits = vendor_fruits_data['fruit'].unique().tolist()
                        fruit_r = st.selectbox("Fruit *", unique_fruits)
                except:
                    fruit_r = st.text_input("Fruit Name", value="APPLE")
                
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
# ---------- Tab 4: Vendor Payments ----------
with tabs[4]:
    st.header("Record Fruit Payments")
    
    st.info("💡 Record actual payments received for fruit sold (NOT box deposits)")
    
    vendors_df = list_vendors()
    
    if vendors_df.empty:
        st.warning("⚠️ Please add vendors first")
    else:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("New Payment")
            
            # MOVE VENDOR SELECTION OUTSIDE FORM
            pdate = st.date_input("Payment Date *", value=date.today(), key="payment_date")
            vpay = st.selectbox("Vendor *", vendors_df['name'].tolist(), key="payment_vendor")
            vid = int(vendors_df[vendors_df['name'] == vpay]['id'].iloc[0])
            
            # SHOW METRICS OUTSIDE FORM - Updates immediately when vendor changes
            summary = vendor_summary_table()
            if not summary.empty:
                vendor_row = summary[summary['vendor_id'] == vid]
                if not vendor_row.empty:
                    vendor_due = vendor_row['net_due'].iloc[0]
                    deposits_held = vendor_row['net_deposits_held'].iloc[0]
                    
                    # Display current vendor's dues
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.metric("Amount Due (for fruit)", f"₹{vendor_due:.2f}")
                    with col_b:
                        st.metric("Box Deposits Held", f"₹{deposits_held:.2f}", 
                                 help="Security deposit for boxes")
            
            st.divider()
            
            # ONLY PAYMENT AMOUNT AND NOTE INSIDE FORM
            with st.form("payment_form", clear_on_submit=True):
                amount = st.number_input("Payment Amount (₹) *", 
                                        min_value=0.0, 
                                        value=0.0, 
                                        step=100.0,
                                        help="Payment for fruit purchase only")
                pnote = st.text_area("Note (optional)", placeholder="Payment details...")
                
                if st.form_submit_button("💵 Record Payment", type="primary"):
                    if amount > 0:
                        if record_payment(pdate.isoformat(), vid, amount, pnote):
                            st.success(f"✅ Payment of ₹{amount} recorded from {vpay}")
                            st.rerun()
                    else:
                        st.error("Payment amount must be greater than 0")
        
        with col2:
            st.subheader("Recent Payments")
            try:
                recent_p_response = supabase.table("payments")\
                    .select("dt, vendor_id, amount, note")\
                    .order("dt", desc=True)\
                    .limit(50)\
                    .execute()
                recent_p = pd.DataFrame(recent_p_response.data)
                
                if not recent_p.empty:
                    # Map vendor names
                    vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                    recent_p['Vendor'] = recent_p['vendor_id'].map(vendor_map)
                    recent_p = recent_p[['dt', 'Vendor', 'amount', 'note']]
                    recent_p.columns = ['Date', 'Vendor', 'Amount', 'Note']
                    recent_p['Amount'] = recent_p['Amount'].apply(lambda x: f"₹{x:.2f}")
                    st.dataframe(recent_p, use_container_width=True, hide_index=True)
                else:
                    st.info("No payments recorded yet")
            except:
                st.info("No payments recorded yet")

# ---------- Tab 5: Vendor Dues ----------
with tabs[5]:
    st.header("Vendor Dues Summary")
    
    st.info("💡 Amount Due = Total Sales - Payments (Box deposits tracked separately)")
    
    summary = vendor_summary_table()
    
    if summary.empty:
        st.info("No transactions yet")
    else:
        total_sales = summary['total_sales'].sum()
        total_due = summary['net_due'].sum()
        total_paid = summary['payments'].sum()
        total_deposits = summary['net_deposits_held'].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sales", f"₹{total_sales:.2f}")
        col2.metric("Total Paid", f"₹{total_paid:.2f}")
        col3.metric("Total Due", f"₹{total_due:.2f}")
        col4.metric("Box Deposits Held", f"₹{total_deposits:.2f}")
        
        st.divider()
        
        summary_display = summary[['vendor_name','total_sales','payments','net_due','deposits_collected','deposits_refunded','net_deposits_held']].copy()
        summary_display.columns = ['Vendor', 'Total Sales (₹)', 'Payments (₹)', 'Amount Due (₹)', 
                                   'Deposits Collected (₹)', 'Deposits Refunded (₹)', 'Net Deposits Held (₹)']
        
        for col in ['Total Sales (₹)', 'Payments (₹)', 'Amount Due (₹)', 'Deposits Collected (₹)', 
                    'Deposits Refunded (₹)', 'Net Deposits Held (₹)']:
            summary_display[col] = summary_display[col].apply(lambda x: f"₹{x:.2f}")
        
        st.dataframe(summary_display, use_container_width=True, hide_index=True)
        
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
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", value=date.today().replace(day=1))
    with col2:
        end_date = st.date_input("To Date", value=date.today())
    
    try:
        sales_response = supabase.table("sales")\
            .select("dt, vendor_id, fruit, boxes, price_per_box, total_price, box_deposit_collected")\
            .gte("dt", start_date.isoformat())\
            .lte("dt", end_date.isoformat())\
            .order("dt", desc=True)\
            .execute()
        
        sales_df = pd.DataFrame(sales_response.data)
        
        if sales_df.empty:
            st.info("No sales in selected date range")
        else:
            # Map vendor names
            vendors_df = list_vendors()
            vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
            sales_df['Vendor'] = sales_df['vendor_id'].map(vendor_map)
            
            total_revenue = float(sales_df['total_price'].sum())
            cogs = compute_cogs_for_sales(sales_df.rename(columns={'boxes':'boxes','fruit':'fruit'}))
            pnl = total_revenue - cogs
            total_deposits = float(sales_df['box_deposit_collected'].sum())
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Revenue", f"₹{total_revenue:.2f}")
            col2.metric("COGS", f"₹{cogs:.2f}")
            col3.metric("P&L", f"₹{pnl:.2f}", delta=f"{(pnl/cogs)*100:.1f}% margin" if cogs > 0 else "")
            col4.metric("Box Deposits", f"₹{total_deposits:.2f}")
            
            st.divider()
            
            sales_display = sales_df[['dt', 'Vendor', 'fruit', 'boxes', 'price_per_box', 'total_price', 'box_deposit_collected']].copy()
            sales_display.columns = ['Date', 'Vendor', 'Fruit', 'Boxes', 'Price/Box', 'Total', 'Box Deposit']
            
            for col in ['Price/Box', 'Total', 'Box Deposit']:
                sales_display[col] = sales_display[col].apply(lambda x: f"₹{x:.2f}")
            
            st.subheader("Sales Details")
            st.dataframe(sales_display, use_container_width=True, hide_index=True)
            
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
    except:
        st.info("No sales in selected date range")

# ---------- Tab 7: Vendor Ledger ----------
with tabs[7]:
    st.header("Vendor Ledger")
    
    st.info("💡 Running Due = Amount owed for fruit | Running Deposits = Box deposits held")
    
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
            final_due = ledger['running_due'].iloc[-1]
            final_deposits = ledger['running_deposits'].iloc[-1]
            
            col1, col2 = st.columns(2)
            col1.metric("Amount Due (Fruit)", f"₹{final_due:.2f}")
            col2.metric("Box Deposits Held", f"₹{final_deposits:.2f}")
            
            st.divider()
            
            ledger_display = ledger.copy()
            ledger_display['sale_amount'] = ledger_display['sale_amount'].apply(lambda x: f"₹{x:.2f}")
            ledger_display['deposit'] = ledger_display['deposit'].apply(lambda x: f"₹{x:.2f}")
            ledger_display['running_due'] = ledger_display['running_due'].apply(lambda x: f"₹{x:.2f}")
            ledger_display['running_deposits'] = ledger_display['running_deposits'].apply(lambda x: f"₹{x:.2f}")
            
            ledger_display.columns = ['Date', 'Type', 'Fruit', 'Qty', 'Sale/Payment', 'Box Deposit', 
                                     'Note', 'Running Due', 'Running Deposits']
            
            st.dataframe(ledger_display, use_container_width=True, hide_index=True)
            
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

st.divider()
st.caption("🍎 DBF Fruit Manager v3.0 - Cloud Persistent Storage with Supabase")
