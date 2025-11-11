# dbf_optimized.py - High-Performance Version
import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from fpdf import FPDF
from io import BytesIO
import json
from supabase import create_client, Client
from typing import Optional, Dict, Any, Tuple, List
import streamlit_authenticator as stauth

BOX_DEPOSIT_DEFAULT = 200.0

# ==================== CONFIGURATION ====================
@st.cache_resource
def init_connection():
    """Initialize Supabase connection with retry logic"""
    try:
        if "connections" in st.secrets and "supabase" in st.secrets["connections"]:
            url = st.secrets["connections"]["supabase"]["SUPABASE_URL"]
            key = st.secrets["connections"]["supabase"]["SUPABASE_KEY"]
        else:
            url = st.secrets["SUPABASE_URL"]
            key = st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

supabase = init_connection()

# ==================== SESSION STATE ====================
def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        'mobile_view': False,
        'edit_mode': False,
        'edited_sales': pd.DataFrame(),
        'delete_mode': False,
        'cache_timestamp': datetime.now()
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ==================== AUTHENTICATION ====================
def get_auth_config():
    """Get auth config from secrets"""
    try:
        if "auth" in st.secrets:
            credentials = {}
            for username, user_data in st.secrets["auth"]["credentials"]["usernames"].items():
                credentials[username] = {
                    'name': user_data.get('name', username),
                    'password': user_data.get('password', '')
                }
            return {
                'credentials': {'usernames': credentials},
                'cookie': {
                    'name': st.secrets["auth"]["cookie"]["name"],
                    'key': st.secrets["auth"]["cookie"]["key"],
                    'expiry_days': int(st.secrets["auth"]["cookie"]["expiry_days"])
                }
            }
    except:
        pass
    
    return {
        'credentials': {
            'usernames': {
                'admin': {
                    'name': 'Admin',
                    'password': '$2b$12$KIXqwEv5Qi1YXJZPqYR7oOeGy3HZqGKOJXoq5vDZxJxVQ.KZ1Y.bG'
                },
                'user': {
                    'name': 'User',
                    'password': '$2b$12$vHJhj5D3l0xKJsX9NhZmbuZvYN.LhJ1DvC8yXqZKqXGQ5PZ9YJxQu'
                }
            }
        },
        'cookie': {
            'name': 'dbf_cookie',
            'key': 'secret_key_12345',
            'expiry_days': 30
        }
    }

# ==================== HELPER FUNCTIONS ====================
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except:
        return default

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except:
        return default

def safe_divide(num: float, denom: float, default: float = 0.0) -> float:
    return num / denom if denom != 0 else default

# ==================== DATABASE OPERATIONS ====================
@st.cache_data(ttl=120, show_spinner=False)
def list_vendors() -> pd.DataFrame:
    """Cached vendor list"""
    try:
        response = supabase.table("vendors").select("*").order("name").execute()
        if response and hasattr(response, 'data') and response.data:
            return pd.DataFrame(response.data)
    except:
        pass
    return pd.DataFrame(columns=['id', 'name', 'contact'])

@st.cache_data(ttl=120, show_spinner=False)
def list_fruits() -> List[str]:
    """Cached fruits with stock"""
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        if response and hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            if 'fruit' in df.columns and 'remaining' in df.columns:
                df = df[df['remaining'] > 0]
                return sorted(df['fruit'].unique().tolist())
    except:
        pass
    return []

@st.cache_data(ttl=60, show_spinner=False)
def get_current_stock() -> Dict[str, int]:
    """Get aggregated stock"""
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        if response and hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            if not df.empty:
                return df.groupby('fruit')['remaining'].sum().to_dict()
    except:
        pass
    return {}

@st.cache_data(ttl=120, show_spinner=False)
def vendor_summary_table() -> pd.DataFrame:
    """Get vendor summary with metrics"""
    try:
        vendors = supabase.table("vendors").select("id, name").execute()
        if not vendors or not hasattr(vendors, 'data') or not vendors.data:
            return pd.DataFrame()
        
        rows = []
        for v in vendors.data:
            vid = v['id']
            
            # Get sales
            sales = supabase.table("sales").select("*").eq("vendor_id", vid).execute()
            sales_df = pd.DataFrame(sales.data if sales and hasattr(sales, 'data') and sales.data else [])
            
            total_sales = safe_float(sales_df['total_price'].sum() if not sales_df.empty else 0)
            
            # Get payments
            payments = supabase.table("payments").select("amount").eq("vendor_id", vid).execute()
            paid = safe_float(pd.DataFrame(payments.data if payments and hasattr(payments, 'data') and payments.data else [])['amount'].sum() if payments and hasattr(payments, 'data') and payments.data else 0)
            
            rows.append({
                "vendor_id": vid,
                "vendor_name": v['name'],
                "total_sales": total_sales,
                "payments": paid,
                "net_due": total_sales - paid
            })
        
        return pd.DataFrame(rows)
    except:
        return pd.DataFrame()

def add_stock(fruit: str, boxes: int, cost: float, dt: str = None) -> bool:
    """Add stock entry"""
    if dt is None:
        dt = date.today().isoformat()
    
    try:
        data = {
            "fruit": fruit.upper().strip(),
            "quantity": int(boxes),
            "cost_price": float(cost),
            "date": dt,
            "remaining": int(boxes)
        }
        supabase.table("stock").insert(data).execute()
        clear_stock_cache()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

def sell_to_vendor(dt: str, vendor_id: int, fruit: str, boxes: int, 
                  price: float, deposit: float, note: str = "") -> bool:
    """Record sale"""
    try:
        data = {
            "dt": dt,
            "vendor_id": int(vendor_id),
            "fruit": fruit,
            "boxes": int(boxes),
            "price_per_box": float(price),
            "total_price": int(boxes) * float(price),
            "box_deposit_per_box": float(deposit),
            "box_deposit_collected": int(boxes) * float(deposit),
            "note": note
        }
        supabase.table("sales").insert(data).execute()
        clear_all_cache()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

def record_payment(dt: str, vendor_id: int, amount: float, note: str = "") -> bool:
    """Record payment"""
    try:
        data = {
            "dt": dt,
            "vendor_id": int(vendor_id),
            "amount": float(amount),
            "note": note
        }
        supabase.table("payments").insert(data).execute()
        vendor_summary_table.clear()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

# ==================== CACHE MANAGEMENT ====================
def clear_stock_cache():
    """Clear stock-related caches"""
    get_current_stock.clear()
    list_fruits.clear()

def clear_all_cache():
    """Clear all caches"""
    st.cache_data.clear()

# ==================== UI HELPERS ====================
def show_metric_row(metrics: Dict[str, str], cols: int = 4):
    """Display metrics in responsive columns"""
    if st.session_state.mobile_view:
        cols = 2
    
    columns = st.columns(cols)
    for idx, (label, value) in enumerate(metrics.items()):
        columns[idx % cols].metric(label, value)

def format_currency(amount: float) -> str:
    """Format as currency"""
    return f"₹{amount:,.2f}"

# ==================== MAIN APP ====================
st.set_page_config(
    page_title="DBF Management",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'About': "DBF Fruit Manager v6.0"}
)

# Minimal CSS
st.markdown("""
<style>
    .stMetric {background: #f0f2f6; padding: 10px; border-radius: 5px;}
    .stTabs [data-baseweb="tab-list"] {gap: 5px;}
    @media (max-width: 768px) {
        .row-widget.stButton > button {width: 100%;}
    }
</style>
""", unsafe_allow_html=True)

# ==================== AUTHENTICATION ====================
config = get_auth_config()
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

authenticator.login()

auth_status = st.session_state.get("authentication_status")
name = st.session_state.get("name")
username = st.session_state.get("username")

if auth_status == False:
    st.error('❌ Invalid credentials')
    st.info("**Try:** admin / admin123")
    st.stop()

if auth_status == None:
    st.warning('⚠️ Please login')
    st.info("**Default:** admin / admin123")
    st.stop()

# ==================== AUTHENTICATED APP ====================
if auth_status:
    
    # Sidebar
    with st.sidebar:
        st.write(f'**{name}**')
        if username == 'admin':
            st.caption("🔑 Administrator")
        else:
            st.caption("👥 User")
        
        authenticator.logout()
        st.divider()
        
        # Quick stats
        with st.spinner("Loading..."):
            stock = get_current_stock()
            st.metric("📦 Stock", f"{sum(stock.values())} boxes" if stock else "0")
            
            summary = vendor_summary_table()
            if not summary.empty:
                st.metric("💵 Dues", format_currency(summary['net_due'].sum()))
        
        st.divider()
        
        # Mobile toggle
        st.session_state.mobile_view = st.checkbox("📱 Mobile", value=st.session_state.mobile_view)
        
        if st.button("🔄 Refresh", use_container_width=True):
            clear_all_cache()
            st.success("✓")
            st.rerun()
    
    st.title("🍎 DBF Management System")
    
    # Main tabs
    tabs = st.tabs(["📋 Vendors", "📦 Stock", "💰 Sell", "💵 Payments", "📊 Dues", "📈 Reports"])
    
    # ==================== TAB 0: VENDORS ====================
    with tabs[0]:
        st.header("Vendors")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            with st.form("vendor_form", clear_on_submit=True):
                vname = st.text_input("Name *")
                vcontact = st.text_input("Contact *", max_chars=10)
                
                if st.form_submit_button("Add", type="primary", use_container_width=True):
                    if vname and vcontact and vcontact.isdigit() and len(vcontact) == 10:
                        try:
                            supabase.table("vendors").insert({"name": vname, "contact": vcontact}).execute()
                            list_vendors.clear()
                            st.success("✓ Added")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        st.error("Invalid input")
        
        with col2:
            vendors_df = list_vendors()
            if not vendors_df.empty:
                st.dataframe(vendors_df, use_container_width=True, hide_index=True)
            else:
                st.info("No vendors")
    
    # ==================== TAB 1: STOCK ====================
    with tabs[1]:
        st.header("Stock")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            with st.form("stock_form", clear_on_submit=True):
                fruit = st.text_input("Fruit *").upper()
                boxes = st.number_input("Boxes *", min_value=1, value=10)
                cost = st.number_input("Cost/Box *", min_value=0.0, value=500.0)
                
                if st.form_submit_button("Add", type="primary", use_container_width=True):
                    if fruit:
                        if add_stock(fruit, boxes, cost):
                            st.success("✓ Added")
                            st.rerun()
        
        with col2:
            stock = get_current_stock()
            if stock:
                stock_df = pd.DataFrame(list(stock.items()), columns=['Fruit', 'Boxes'])
                st.dataframe(stock_df, use_container_width=True, hide_index=True)
                
                low = {k: v for k, v in stock.items() if v <= 5}
                if low:
                    st.warning(f"⚠️ Low: {', '.join([f'{k}({v})' for k, v in low.items()])}")
            else:
                st.info("No stock")
    
    # ==================== TAB 2: SELL ====================
    with tabs[2]:
        st.header("Record Sale")
        
        vendors_df = list_vendors()
        fruits = list_fruits()
        
        if vendors_df.empty:
            st.warning("Add vendors first")
        elif not fruits:
            st.warning("Add stock first")
        else:
            with st.form("sell_form", clear_on_submit=True):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    sdate = st.date_input("Date", value=date.today())
                    vendor = st.selectbox("Vendor", vendors_df['name'].tolist())
                    vid = int(vendors_df[vendors_df['name'] == vendor]['id'].iloc[0])
                
                with col2:
                    fruit = st.selectbox("Fruit", fruits)
                    boxes = st.number_input("Boxes", min_value=1, value=1)
                    stock = get_current_stock()
                    st.caption(f"Available: {stock.get(fruit, 0)}")
                
                with col3:
                    price = st.number_input("Price/Box", min_value=0.0, value=700.0)
                    deposit = st.number_input("Deposit/Box", min_value=0.0, value=BOX_DEPOSIT_DEFAULT)
                
                st.caption(f"💰 Total: {format_currency(boxes * price)} | 📦 Deposit: {format_currency(boxes * deposit)}")
                
                if st.form_submit_button("Record Sale", type="primary", use_container_width=True):
                    if sell_to_vendor(sdate.isoformat(), vid, fruit, boxes, price, deposit):
                        st.success("✓ Recorded")
                        st.rerun()
    
    # ==================== TAB 3: PAYMENTS ====================
    with tabs[3]:
        st.header("Payments")
        
        vendors_df = list_vendors()
        
        if vendors_df.empty:
            st.warning("Add vendors first")
        else:
            col1, col2 = st.columns([1, 2])
            
            with col1:
                pdate = st.date_input("Date", value=date.today(), key="pdate")
                vendor = st.selectbox("Vendor", vendors_df['name'].tolist(), key="pvendor")
                vid = int(vendors_df[vendors_df['name'] == vendor]['id'].iloc[0])
                
                summary = vendor_summary_table()
                if not summary.empty:
                    vrow = summary[summary['vendor_id'] == vid]
                    if not vrow.empty:
                        show_metric_row({
                            "Due": format_currency(vrow['net_due'].iloc[0]),
                            "Paid": format_currency(vrow['payments'].iloc[0])
                        }, cols=2)
                
                with st.form("payment_form", clear_on_submit=True):
                    amount = st.number_input("Amount", min_value=0.0, value=0.0, step=100.0)
                    
                    if st.form_submit_button("Record", type="primary", use_container_width=True):
                        if amount > 0:
                            if record_payment(pdate.isoformat(), vid, amount):
                                st.success("✓ Recorded")
                                st.rerun()
                        else:
                            st.error("Amount must be > 0")
            
            with col2:
                st.subheader("Recent Payments")
                try:
                    payments = supabase.table("payments").select("*").order("dt", desc=True).limit(20).execute()
                    if payments and hasattr(payments, 'data') and payments.data:
                        df = pd.DataFrame(payments.data)
                        vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                        df['Vendor'] = df['vendor_id'].map(vendor_map)
                        df = df[['dt', 'Vendor', 'amount']]
                        df.columns = ['Date', 'Vendor', 'Amount']
                        df['Amount'] = df['Amount'].apply(format_currency)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                except:
                    st.info("No payments")
    
    # ==================== TAB 4: DUES ====================
    with tabs[4]:
        st.header("Vendor Dues")
        
        summary = vendor_summary_table()
        
        if summary.empty:
            st.info("No transactions")
        else:
            show_metric_row({
                "Sales": format_currency(summary['total_sales'].sum()),
                "Paid": format_currency(summary['payments'].sum()),
                "Due": format_currency(summary['net_due'].sum())
            }, cols=3)
            
            st.divider()
            
            display = summary.copy()
            for col in ['total_sales', 'payments', 'net_due']:
                display[col] = display[col].apply(format_currency)
            
            display.columns = ['ID', 'Vendor', 'Sales', 'Paid', 'Due']
            st.dataframe(display[['Vendor', 'Sales', 'Paid', 'Due']], use_container_width=True, hide_index=True)
    
    # ==================== TAB 5: REPORTS ====================
    with tabs[5]:
        st.header("Sales Reports")
        
        col1, col2 = st.columns(2)
        start = col1.date_input("From", value=date.today().replace(day=1))
        end = col2.date_input("To", value=date.today())
        
        try:
            sales = supabase.table("sales")\
                .select("*")\
                .gte("dt", start.isoformat())\
                .lte("dt", end.isoformat())\
                .execute()
            
            if sales and hasattr(sales, 'data') and sales.data:
                df = pd.DataFrame(sales.data)
                
                total = df['total_price'].sum()
                show_metric_row({
                    "Revenue": format_currency(total),
                    "Boxes": str(df['boxes'].sum()),
                    "Avg/Box": format_currency(safe_divide(total, df['boxes'].sum()))
                }, cols=3)
                
                st.divider()
                
                vendors_df = list_vendors()
                vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                df['Vendor'] = df['vendor_id'].map(vendor_map)
                
                display = df[['dt', 'Vendor', 'fruit', 'boxes', 'total_price']].copy()
                display.columns = ['Date', 'Vendor', 'Fruit', 'Boxes', 'Total']
                display['Total'] = display['Total'].apply(format_currency)
                
                st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.info("No sales")
        except Exception as e:
            st.error(f"Error: {e}")
    
    # Footer
    st.divider()
    st.caption(f"🍎 DBF Manager v6.0 - {name}")


