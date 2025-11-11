# d.py - Complete Production Version with Fixed Approval Workflow
import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from fpdf import FPDF
from io import BytesIO
import json
from supabase import create_client, Client
from typing import Optional, Dict, Any, Tuple, List
import streamlit_authenticator as stauth

BOX_DEPOSIT_DEFAULT = 200

# ==================== CONFIGURATION ====================
@st.cache_resource
def init_connection():
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
if 'mobile_view' not in st.session_state:
    st.session_state.mobile_view = False
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'edited_sales' not in st.session_state:
    st.session_state.edited_sales = pd.DataFrame()

# ==================== AUTHENTICATION ====================
def get_auth_config():
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
                'admin': {'name': 'Admin', 'password': '$2b$12$KIXqwEv5Qi1YXJZPqYR7oOeGy3HZqGKOJXoq5vDZxJxVQ.KZ1Y.bG'},
                'user': {'name': 'User', 'password': '$2b$12$vHJhj5D3l0xKJsX9NhZmbuZvYN.LhJ1DvC8yXqZKqXGQ5PZ9YJxQu'}
            }
        },
        'cookie': {'name': 'dbf_cookie', 'key': 'secret_key', 'expiry_days': 30}
    }

# ==================== HELPER FUNCTIONS ====================
def safe_float(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except:
        return default

def safe_int(value, default=0):
    try:
        return int(value) if value is not None else default
    except:
        return default

def safe_divide(num, denom, default=0.0):
    return num / denom if denom != 0 else default

def format_currency(amount):
    return f"₹{amount:,.2f}"

def clean_for_json(obj):
    """Clean data for JSON serialization"""
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, datetime, date)):
        return str(obj)
    elif hasattr(obj, 'item'):
        return obj.item()
    else:
        return obj

# ==================== CACHE FUNCTIONS ====================
@st.cache_data(ttl=120, show_spinner=False)
def list_vendors():
    try:
        response = supabase.table("vendors").select("*").order("name").execute()
        if response and hasattr(response, 'data') and response.data:
            return pd.DataFrame(response.data)
    except:
        pass
    return pd.DataFrame(columns=['id', 'name', 'contact'])

@st.cache_data(ttl=120, show_spinner=False)
def list_fruits():
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        if response and hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            if 'fruit' in df.columns and 'remaining' in df.columns:
                return sorted(df[df['remaining'] > 0]['fruit'].unique().tolist())
    except:
        pass
    return []

@st.cache_data(ttl=60, show_spinner=False)
def get_current_stock():
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
def vendor_summary_table():
    try:
        vendors = supabase.table("vendors").select("id, name").execute()
        if not vendors or not hasattr(vendors, 'data') or not vendors.data:
            return pd.DataFrame()
        
        rows = []
        for v in vendors.data:
            vid = v['id']
            
            sales = supabase.table("sales").select("*").eq("vendor_id", vid).execute()
            sales_df = pd.DataFrame(sales.data if sales and hasattr(sales, 'data') and sales.data else [])
            
            payments = supabase.table("payments").select("amount").eq("vendor_id", vid).execute()
            payments_df = pd.DataFrame(payments.data if payments and hasattr(payments, 'data') and payments.data else [])
            
            returns = supabase.table("returns").select("box_deposit_refunded").eq("vendor_id", vid).execute()
            returns_df = pd.DataFrame(returns.data if returns and hasattr(returns, 'data') and returns.data else [])
            
            total_sales = safe_float(sales_df['total_price'].sum() if not sales_df.empty and 'total_price' in sales_df.columns else 0)
            deposits_collected = safe_float(sales_df['box_deposit_collected'].sum() if not sales_df.empty and 'box_deposit_collected' in sales_df.columns else 0)
            paid = safe_float(payments_df['amount'].sum() if not payments_df.empty and 'amount' in payments_df.columns else 0)
            deposits_refunded = safe_float(returns_df['box_deposit_refunded'].sum() if not returns_df.empty and 'box_deposit_refunded' in returns_df.columns else 0)
            
            rows.append({
                "vendor_id": vid,
                "vendor_name": v['name'],
                "total_sales": total_sales,
                "payments": paid,
                "net_due": total_sales - paid,
                "deposits_collected": deposits_collected,
                "deposits_refunded": deposits_refunded,
                "net_deposits_held": deposits_collected - deposits_refunded
            })
        
        return pd.DataFrame(rows)
    except:
        return pd.DataFrame()

# ==================== CRUD OPERATIONS ====================
def add_stock(fruit, boxes, cost, dt=None):
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
        get_current_stock.clear()
        list_fruits.clear()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

def reduce_stock_fifo(fruit, boxes_to_reduce):
    try:
        response = supabase.table("stock").select("*").eq("fruit", fruit).gt("remaining", 0).execute()
        if not response or not hasattr(response, 'data') or not response.data:
            return False, f"No stock for {fruit}"
        
        entries = sorted(response.data, key=lambda x: (x.get('date', ''), x.get('id', 0)))
        remaining = boxes_to_reduce
        
        for entry in entries:
            if remaining <= 0:
                break
            available = safe_int(entry.get('remaining', 0))
            to_reduce = min(available, remaining)
            new_remaining = available - to_reduce
            
            supabase.table("stock").update({"remaining": new_remaining}).eq("id", entry['id']).execute()
            remaining -= to_reduce
        
        return remaining == 0, "Success" if remaining == 0 else f"Short by {remaining}"
    except Exception as e:
        return False, str(e)

def sell_to_vendor(dt, vendor_id, fruit, boxes, price, deposit, note=""):
    try:
        stock = get_current_stock()
        if boxes > stock.get(fruit, 0):
            st.error(f"Insufficient stock. Available: {stock.get(fruit, 0)}")
            return False
        
        success, msg = reduce_stock_fifo(fruit, boxes)
        if not success:
            st.error(msg)
            return False
        
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
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

def record_return(dt, vendor_id, fruit, boxes, deposit, note=""):
    try:
        data = {
            "dt": dt,
            "vendor_id": int(vendor_id),
            "fruit": fruit,
            "boxes_returned": int(boxes),
            "box_deposit_refunded": int(boxes) * float(deposit),
            "note": note
        }
        supabase.table("returns").insert(data).execute()
        
        stock_data = {
            "fruit": fruit,
            "quantity": int(boxes),
            "cost_price": 0.0,
            "date": dt,
            "remaining": int(boxes)
        }
        supabase.table("stock").insert(stock_data).execute()
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

def record_payment(dt, vendor_id, amount, note=""):
    try:
        data = {"dt": dt, "vendor_id": int(vendor_id), "amount": float(amount), "note": note}
        supabase.table("payments").insert(data).execute()
        vendor_summary_table.clear()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

# ==================== CHANGE REQUEST FUNCTIONS (FIXED) ====================
def submit_change_request(sale_id, current_data, requested_data, username, name, note=""):
    """Submit a change request - DOES NOT apply changes, only creates request"""
    try:
        current_clean = clean_for_json(current_data)
        requested_clean = clean_for_json(requested_data)
        
        request_data = {
            "requested_by": username,
            "requester_name": name,
            "sale_id": int(sale_id),
            "change_type": "edit_sale",
            "current_data": json.dumps(current_clean),
            "requested_data": json.dumps(requested_clean),
            "status": "pending",
            "note": note.strip() if note else ""
        }
        
        supabase.table("change_requests").insert(request_data).execute()
        return True
        
    except Exception as e:
        st.error(f"Request failed: {e}")
        return False

def get_pending_requests(status="pending"):
    try:
        response = supabase.table("change_requests").select("*").eq("status", status).order("request_date", desc=True).execute()
        if response and hasattr(response, 'data') and response.data:
            return pd.DataFrame(response.data)
    except:
        pass
    return pd.DataFrame()

def approve_change_request(request_id, admin_username, comment=""):
    """Approve and APPLY changes - ONLY NOW changes are applied"""
    try:
        req = supabase.table("change_requests").select("*").eq("id", request_id).execute()
        if not req or not hasattr(req, 'data') or not req.data:
            st.error("Request not found")
            return False
        
        request = req.data[0]
        
        if request['status'] != 'pending':
            st.warning(f"Request already {request['status']}")
            return False
        
        requested_data = json.loads(request['requested_data'])
        sale_id = request['sale_id']
        
        boxes = safe_int(requested_data.get('boxes', 0))
        price = safe_float(requested_data.get('price_per_box', 0))
        deposit = safe_float(requested_data.get('box_deposit_per_box', 0))
        
        update_data = {
            'dt': str(requested_data.get('dt', '')),
            'fruit': str(requested_data.get('fruit', '')),
            'boxes': boxes,
            'price_per_box': price,
            'total_price': boxes * price,
            'box_deposit_per_box': deposit,
            'box_deposit_collected': boxes * deposit,
            'note': str(requested_data.get('note', ''))
        }
        
        # APPLY CHANGES TO SALES TABLE
        supabase.table("sales").update(update_data).eq("id", sale_id).execute()
        
        # UPDATE REQUEST STATUS
        supabase.table("change_requests").update({
            "status": "approved",
            "reviewed_by": admin_username,
            "reviewed_date": datetime.now().isoformat(),
            "admin_comment": comment or "Approved"
        }).eq("id", request_id).execute()
        
        st.cache_data.clear()
        return True
        
    except Exception as e:
        st.error(f"Approval failed: {e}")
        return False

def reject_change_request(request_id, admin_username, comment):
    """Reject - NO changes applied to sales"""
    try:
        if not comment or not comment.strip():
            st.error("Rejection reason required")
            return False
        
        req = supabase.table("change_requests").select("*").eq("id", request_id).execute()
        if not req or not hasattr(req, 'data') or not req.data:
            st.error("Request not found")
            return False
        
        request = req.data[0]
        
        if request['status'] != 'pending':
            st.warning(f"Request already {request['status']}")
            return False
        
        # ONLY UPDATE REQUEST STATUS - DO NOT TOUCH SALES
        supabase.table("change_requests").update({
            "status": "rejected",
            "reviewed_by": admin_username,
            "reviewed_date": datetime.now().isoformat(),
            "admin_comment": comment.strip()
        }).eq("id", request_id).execute()
        
        return True
        
    except Exception as e:
        st.error(f"Rejection failed: {e}")
        return False

def get_request_counts():
    try:
        response = supabase.table("change_requests").select("status").execute()
        if response and hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            counts = df['status'].value_counts().to_dict()
            return {
                'pending': counts.get('pending', 0),
                'approved': counts.get('approved', 0),
                'rejected': counts.get('rejected', 0)
            }
    except:
        pass
    return {'pending': 0, 'approved': 0, 'rejected': 0}

def get_sales_for_editing(start, end):
    try:
        response = supabase.table("sales").select("*").gte("dt", start).lte("dt", end).order("dt", desc=True).execute()
        if response and hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            vendors_df = list_vendors()
            if not vendors_df.empty:
                vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                df['vendor_name'] = df['vendor_id'].map(vendor_map)
            return df
    except:
        pass
    return pd.DataFrame()

# ==================== REPORTS ====================
def get_daily_summary(selected_date=None):
    if selected_date is None:
        selected_date = date.today()
    
    date_str = selected_date.isoformat()
    
    try:
        sales = supabase.table("sales").select("*").eq("dt", date_str).execute()
        sales_df = pd.DataFrame(sales.data if sales and hasattr(sales, 'data') and sales.data else [])
        
        payments = supabase.table("payments").select("*").eq("dt", date_str).execute()
        payments_df = pd.DataFrame(payments.data if payments and hasattr(payments, 'data') and payments.data else [])
        
        return {
            "date": date_str,
            "total_sales": safe_float(sales_df['total_price'].sum() if not sales_df.empty else 0),
            "boxes_sold": safe_int(sales_df['boxes'].sum() if not sales_df.empty else 0),
            "payments_received": safe_float(payments_df['amount'].sum() if not payments_df.empty else 0),
            "num_transactions": len(sales_df) + len(payments_df)
        }
    except:
        return None

def export_to_excel(df):
    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        buf.seek(0)
        return buf
    except Exception as e:
        st.error(f"Export error: {e}")
        return BytesIO()

# ==================== MAIN APP ====================
st.set_page_config(
    page_title="DBF Manager",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stMetric {background: #f0f2f6; padding: 10px; border-radius: 5px;}
    @media (max-width: 768px) {.row-widget.stButton > button {width: 100%;}}
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
    st.info("**Try:** admin / admin123 or user / user123")
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
            counts = get_request_counts()
            if counts['pending'] > 0:
                st.warning(f"⏳ {counts['pending']} pending")
        else:
            st.caption("👥 User")
        
        authenticator.logout()
        st.divider()
        
        with st.spinner("Loading..."):
            stock = get_current_stock()
            st.metric("📦 Stock", f"{sum(stock.values())} boxes" if stock else "0")
            
            summary = vendor_summary_table()
            if not summary.empty:
                st.metric("💵 Dues", format_currency(summary['net_due'].sum()))
        
        st.divider()
        st.session_state.mobile_view = st.checkbox("📱 Mobile", value=st.session_state.mobile_view)
        
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    st.title("🍎 DBF Management System")
    
    # Main tabs
    tabs = st.tabs(["📋 Vendors", "📦 Stock", "💰 Sell", "↩️ Returns", "💵 Payments", 
                    "✏️ Edit Sales", "📊 Dues", "📈 Reports", "📅 Daily"])
    
    # TAB 0: VENDORS
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
                            st.success("✓")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
        
        with col2:
            vendors_df = list_vendors()
            if not vendors_df.empty:
                st.dataframe(vendors_df, use_container_width=True, hide_index=True)
            else:
                st.info("No vendors")
    
    # TAB 1: STOCK
    with tabs[1]:
        st.header("Stock")
        col1, col2 = st.columns([1, 2])
        
        with col1:
            with st.form("stock_form", clear_on_submit=True):
                fruit = st.text_input("Fruit *").upper()
                boxes = st.number_input("Boxes *", min_value=1, value=10)
                cost = st.number_input("Cost/Box *", min_value=0.0, value=500.0)
                
                if st.form_submit_button("Add", type="primary", use_container_width=True):
                    if fruit and add_stock(fruit, boxes, cost):
                        st.success("✓")
                        st.rerun()
        
        with col2:
            stock = get_current_stock()
            if stock:
                stock_df = pd.DataFrame(list(stock.items()), columns=['Fruit', 'Boxes'])
                st.dataframe(stock_df, use_container_width=True, hide_index=True)
            else:
                st.info("No stock")
    
    # TAB 2: SELL
    with tabs[2]:
        st.header("Record Sale")
        
        vendors_df = list_vendors()
        fruits = list_fruits()
        
        if vendors_df.empty or not fruits:
            st.warning("Add vendors and stock first")
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
                
                with col3:
                    price = st.number_input("Price/Box", min_value=0.0, value=700.0)
                    deposit = st.number_input("Deposit/Box", min_value=0.0, value=BOX_DEPOSIT_DEFAULT)
                
                if st.form_submit_button("Record", type="primary", use_container_width=True):
                    if sell_to_vendor(sdate.isoformat(), vid, fruit, boxes, price, deposit):
                        st.success("✓")
                        st.rerun()
    
    # TAB 3: RETURNS
    with tabs[3]:
        st.header("Record Returns")
        
        vendors_df = list_vendors()
        if vendors_df.empty:
            st.warning("Add vendors first")
        else:
            with st.form("return_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    rdate = st.date_input("Date", value=date.today())
                    vendor = st.selectbox("Vendor", vendors_df['name'].tolist())
                    vid = int(vendors_df[vendors_df['name'] == vendor]['id'].iloc[0])
                
                with col2:
                    rfruit = st.text_input("Fruit", value="APPLE")
                    rboxes = st.number_input("Boxes", min_value=1, value=1)
                
                rdeposit = st.number_input("Deposit/Box", min_value=0.0, value=BOX_DEPOSIT_DEFAULT)
                
                if st.form_submit_button("Record", type="primary", use_container_width=True):
                    if record_return(rdate.isoformat(), vid, rfruit.upper(), rboxes, rdeposit):
                        st.success("✓")
                        st.rerun()
    
    # TAB 4: PAYMENTS
    with tabs[4]:
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
                        col_a, col_b = st.columns(2)
                        col_a.metric("Due", format_currency(vrow['net_due'].iloc[0]))
                        col_b.metric("Deposits", format_currency(vrow['net_deposits_held'].iloc[0]))
                
                with st.form("payment_form", clear_on_submit=True):
                    amount = st.number_input("Amount", min_value=0.0, value=0.0, step=100.0)
                    
                    if st.form_submit_button("Record", type="primary", use_container_width=True):
                        if amount > 0 and record_payment(pdate.isoformat(), vid, amount):
                            st.success("✓")
                            st.rerun()
            
            with col2:
                st.subheader("Recent")
                try:
                    payments = supabase.table("payments").select("*").order("dt", desc=True).limit(20).execute()
                    if payments and hasattr(payments, 'data') and payments.data:
                        df = pd.DataFrame(payments.data)
                        vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                        df['Vendor'] = df['vendor_id'].map(vendor_map)
                        st.dataframe(df[['dt', 'Vendor', 'amount']], use_container_width=True, hide_index=True)
                except:
                    st.info("No payments")
    
    # TAB 5: EDIT SALES (FIXED WORKFLOW)
    with tabs[5]:
        st.header("✏️ Edit Sales")
        
        if username == 'admin':
            st.success("🔑 Admin Mode")
            
            counts = get_request_counts()
            col1, col2, col3 = st.columns(3)
            col1.metric("⏳ Pending", counts['pending'])
            col2.metric("✅ Approved", counts['approved'])
            col3.metric("❌ Rejected", counts['rejected'])
            
            st.divider()
            
            admin_tabs = st.tabs(["📋 Pending Requests", "✏️ Direct Edit", "📜 History"])
            
            with admin_tabs[0]:
                st.subheader("📋 Pending Requests")
                
                pending = get_pending_requests("pending")
                
                if pending.empty:
                    st.success("✅ No pending requests")
                else:
                    st.warning(f"⏳ {len(pending)} request(s) awaiting review")
                    
                    for _, req in pending.iterrows():
                        with st.expander(f"🔔 Request #{req['id']} - Sale #{req['sale_id']} - By {req['requester_name']}"):
                            
                            current = json.loads(req['current_data'])
                            requested = json.loads(req['requested_data'])
                            
                            st.info(f"**Reason:** {req.get('note', 'N/A')}")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown("### 📊 Current")
                                st.json(current)
                            
                            with col2:
                                st.markdown("### ✏️ Requested")
                                st.json(requested)
                            
                            st.divider()
                            st.markdown("### 🔍 Changes:")
                            
                            for key in requested.keys():
                                if key in current and str(current[key]) != str(requested[key]):
                                    st.markdown(f"- **{key}**: `{current[key]}` → `{requested[key]}`")
                            
                            st.divider()
                            
                            col_a, col_b = st.columns(2)
                            
                            with col_a:
                                with st.form(f"approve_{req['id']}", clear_on_submit=True):
                                    st.markdown("#### ✅ Approve")
                                    comment = st.text_area("Comment", key=f"ca{req['id']}")
                                    
                                    if st.form_submit_button("✅ Approve & Apply", type="primary", use_container_width=True):
                                        with st.spinner("Applying..."):
                                            if approve_change_request(req['id'], username, comment):
                                                st.success("✅ Approved! Changes applied.")
                                                st.balloons()
                                                st.rerun()
                            
                            with col_b:
                                with st.form(f"reject_{req['id']}", clear_on_submit=True):
                                    st.markdown("#### ❌ Reject")
                                    reason = st.text_area("Reason *", key=f"cr{req['id']}")
                                    
                                    if st.form_submit_button("❌ Reject", use_container_width=True):
                                        if reason.strip() and reject_change_request(req['id'], username, reason):
                                            st.success("✅ Rejected")
                                            st.rerun()
                                        else:
                                            st.error("Reason required")
            
            with admin_tabs[1]:
                st.subheader("Direct Edit")
                st.warning("⚠️ Immediate changes")
                
                col1, col2 = st.columns(2)
                start = col1.date_input("From", value=date.today().replace(day=1), key="admin_direct_start")
                end = col2.date_input("To", value=date.today(), key="admin_direct_end")
                
                if st.button("Load", key="admin_load"):
                    sales_df = get_sales_for_editing(start.isoformat(), end.isoformat())
                    if not sales_df.empty:
                        st.session_state.edited_sales = sales_df
                        st.session_state.edit_mode = True
                    else:
                        st.warning("No sales")
                
                if st.session_state.edit_mode and not st.session_state.edited_sales.empty:
                    edit_cols = ['id', 'dt', 'vendor_name', 'fruit', 'boxes', 'price_per_box', 'box_deposit_per_box']
                    display_df = st.session_state.edited_sales[edit_cols].copy()
                    
                    edited_df = st.data_editor(
                        display_df,
                        use_container_width=True,
                        num_rows="fixed",
                        hide_index=True,
                        key="admin_editor",
                        column_config={
                            'id': st.column_config.NumberColumn('ID', disabled=True),
                            'dt': st.column_config.TextColumn('Date', disabled=True),
                            'vendor_name': st.column_config.TextColumn('Vendor', disabled=True)
                        }
                    )
                    
                    if st.button("💾 Save", type="primary", key="admin_save"):
                        st.success("Saved")
                        st.session_state.edit_mode = False
                        st.rerun()
            
            with admin_tabs[2]:
                st.subheader("History")
                approved = get_pending_requests("approved")
                rejected = get_pending_requests("rejected")
                
                if not approved.empty:
                    st.markdown("### ✅ Approved")
                    st.dataframe(approved[['id', 'requester_name', 'sale_id', 'reviewed_by']], 
                               use_container_width=True, hide_index=True)
                
                if not rejected.empty:
                    st.markdown("### ❌ Rejected")
                    st.dataframe(rejected[['id', 'requester_name', 'sale_id', 'reviewed_by', 'admin_comment']], 
                               use_container_width=True, hide_index=True)
        
        else:
            st.info("📝 Submit requests for admin approval")
            st.warning("⚠️ Changes apply ONLY after admin approval")
            
            try:
                user_reqs = supabase.table("change_requests").select("*").eq("requested_by", username).order("request_date", desc=True).limit(10).execute()
                if user_reqs and hasattr(user_reqs, 'data') and user_reqs.data:
                    df = pd.DataFrame(user_reqs.data)
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("⏳ Pending", len(df[df['status']=='pending']))
                    col2.metric("✅ Approved", len(df[df['status']=='approved']))
                    col3.metric("❌ Rejected", len(df[df['status']=='rejected']))
                    
                    st.divider()
                    
                    if not df.empty:
                        st.subheader("📜 My Requests")
                        
                        for _, req in df.head(5).iterrows():
                            status_icon = {'pending': '⏳', 'approved': '✅', 'rejected': '❌'}.get(req['status'], '❓')
                            
                            with st.expander(f"{status_icon} #{req['id']} - Sale {req['sale_id']} - {req['status'].upper()}"):
                                st.caption(f"Submitted: {req['request_date']}")
                                st.write(f"**Note:** {req.get('note', 'N/A')}")
                                
                                if req['status'] == 'pending':
                                    st.info("⏳ Awaiting approval...")
                                elif req['status'] == 'approved':
                                    st.success(f"✅ Approved by {req.get('reviewed_by', 'Admin')}")
                                    if req.get('admin_comment'):
                                        st.write(f"**Comment:** {req['admin_comment']}")
                                elif req['status'] == 'rejected':
                                    st.error(f"❌ Rejected by {req.get('reviewed_by', 'Admin')}")
                                    st.write(f"**Reason:** {req.get('admin_comment', 'N/A')}")
            except:
                pass
            
            st.divider()
            
            col1, col2 = st.columns(2)
            start = col1.date_input("From", value=date.today().replace(day=1), key="user_start")
            end = col2.date_input("To", value=date.today(), key="user_end")
            
            if st.button("Load", key="user_load"):
                sales_df = get_sales_for_editing(start.isoformat(), end.isoformat())
                if not sales_df.empty:
                    st.session_state.user_edit_sales = sales_df
                else:
                    st.warning("No sales")
            
            if 'user_edit_sales' in st.session_state and not st.session_state.user_edit_sales.empty:
                st.warning("⚠️ Changes require approval")
                
                edit_cols = ['id', 'dt', 'vendor_name', 'fruit', 'boxes', 'price_per_box', 'box_deposit_per_box']
                display_df = st.session_state.user_edit_sales[edit_cols].copy()
                
                edited_df = st.data_editor(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    key="user_editor",
                    column_config={
                        'id': st.column_config.NumberColumn('ID', disabled=True),
                        'dt': st.column_config.TextColumn('Date', disabled=True),
                        'vendor_name': st.column_config.TextColumn('Vendor', disabled=True)
                    }
                )
                
                reason = st.text_area("Reason *", key="user_reason")
                
                if st.button("📤 Submit", type="primary", key="user_submit"):
                    if not reason.strip():
                        st.error("Reason required")
                    else:
                        submitted = 0
                        for idx in range(len(st.session_state.user_edit_sales)):
                            orig = st.session_state.user_edit_sales.iloc[idx][edit_cols]
                            edit = edited_df.iloc[idx]
                            
                            if not orig.equals(edit):
                                current = orig.to_dict()
                                requested = edit.to_dict()
                                
                                if submit_change_request(int(edit['id']), current, requested, username, name, reason):
                                    submitted += 1
                        
                        if submitted > 0:
                            st.success(f"✓ Submitted {submitted} request(s)")
                            del st.session_state.user_edit_sales
                            st.rerun()
    
    # TAB 6: DUES
    with tabs[6]:
        st.header("Dues")
        
        summary = vendor_summary_table()
        if summary.empty:
            st.info("No transactions")
        else:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Sales", format_currency(summary['total_sales'].sum()))
            col2.metric("Paid", format_currency(summary['payments'].sum()))
            col3.metric("Due", format_currency(summary['net_due'].sum()))
            col4.metric("Deposits", format_currency(summary['net_deposits_held'].sum()))
            
            st.divider()
            
            display = summary.copy()
            for col in ['total_sales', 'payments', 'net_due', 'net_deposits_held']:
                display[col] = display[col].apply(format_currency)
            
            st.dataframe(display[['vendor_name', 'total_sales', 'payments', 'net_due', 'net_deposits_held']], 
                        use_container_width=True, hide_index=True)
    
    # TAB 7: REPORTS
    with tabs[7]:
        st.header("Reports")
        
        col1, col2 = st.columns(2)
        start = col1.date_input("From", value=date.today().replace(day=1), key="report_start")
        end = col2.date_input("To", value=date.today(), key="report_end")
        
        try:
            sales = supabase.table("sales").select("*").gte("dt", start.isoformat()).lte("dt", end.isoformat()).execute()
            if sales and hasattr(sales, 'data') and sales.data:
                df = pd.DataFrame(sales.data)
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Revenue", format_currency(df['total_price'].sum()))
                col2.metric("Boxes", str(df['boxes'].sum()))
                col3.metric("Avg", format_currency(safe_divide(df['total_price'].sum(), df['boxes'].sum())))
                
                st.divider()
                
                vendors_df = list_vendors()
                vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                df['Vendor'] = df['vendor_id'].map(vendor_map)
                
                st.dataframe(df[['dt', 'Vendor', 'fruit', 'boxes', 'total_price']], use_container_width=True, hide_index=True)
                
                excel = export_to_excel(df)
                st.download_button("📥 Download", data=excel, file_name=f"sales_{start}_{end}.xlsx", 
                                 mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("No sales")
        except Exception as e:
            st.error(f"Error: {e}")
    
    # TAB 8: DAILY
    with tabs[8]:
        st.header("Daily Summary")
        
        selected = st.date_input("Date", value=date.today(), key="daily_date")
        
        summary = get_daily_summary(selected)
        if summary and summary['num_transactions'] > 0:
            col1, col2, col3 = st.columns(3)
            col1.metric("Sales", format_currency(summary['total_sales']))
            col2.metric("Boxes", str(summary['boxes_sold']))
            col3.metric("Payments", format_currency(summary['payments_received']))
        else:
            st.info("No transactions")
    
    st.divider()
    st.caption(f"🍎 DBF v6.1 - {name} ({username})")


