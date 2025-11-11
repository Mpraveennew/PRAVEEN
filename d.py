# dbf.py - Complete Production Version with Authentication & Edit Features
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

# -------------------- Authentication Configuration --------------------
def get_auth_config():
    """Get authentication configuration from secrets"""
    try:
        # Try to get from secrets first
        if "auth" in st.secrets:
            credentials = {}
            
            # Build credentials dictionary from secrets
            if "credentials" in st.secrets["auth"]:
                if "usernames" in st.secrets["auth"]["credentials"]:
                    for username, user_data in st.secrets["auth"]["credentials"]["usernames"].items():
                        credentials[username] = {
                            'name': user_data.get('name', username),
                            'password': user_data.get('password', '')
                        }
            
            return {
                'credentials': {
                    'usernames': credentials
                },
                'cookie': {
                    'name': st.secrets["auth"]["cookie"]["name"],
                    'key': st.secrets["auth"]["cookie"]["key"],
                    'expiry_days': int(st.secrets["auth"]["cookie"]["expiry_days"])
                }
            }
    except Exception as e:
        st.error(f"Error loading auth config: {e}")
    
    # Fallback to default credentials
    return {
        'credentials': {
            'usernames': {
                'admin': {
                    'name': 'Admin User',
                    'password': '$2b$12$KIXqwEv5Qi1YXJZPqYR7oOeGy3HZqGKOJXoq5vDZxJxVQ.KZ1Y.bG'
                },
                'user': {
                    'name': 'Regular User',
                    'password': '$2b$12$vHJhj5D3l0xKJsX9NhZmbuZvYN.LhJ1DvC8yXqZKqXGQ5PZ9YJxQu'
                }
            }
        },
        'cookie': {
            'name': 'dbf_auth_cookie',
            'key': 'default_secret_key_12345',
            'expiry_days': 30
        }
    }

# -------------------- Supabase Connection --------------------
@st.cache_resource
def init_connection():
    """Initialize Supabase connection"""
    try:
        # Try nested structure first (your current setup)
        if "connections" in st.secrets and "supabase" in st.secrets["connections"]:
            url = st.secrets["connections"]["supabase"]["SUPABASE_URL"]
            key = st.secrets["connections"]["supabase"]["SUPABASE_KEY"]
        else:
            # Fallback to flat structure
            url = st.secrets["SUPABASE_URL"]
            key = st.secrets["SUPABASE_KEY"]
        
        return create_client(url, key)
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.error("Please check your secrets configuration in Streamlit Cloud Settings")
        st.stop()

supabase = init_connection()

# -------------------- Session State Initialization --------------------
if 'mobile_view' not in st.session_state:
    st.session_state.mobile_view = False
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'edited_sales' not in st.session_state:
    st.session_state.edited_sales = pd.DataFrame()
if 'delete_mode' not in st.session_state:
    st.session_state.delete_mode = False

# -------------------- Helper Functions --------------------
def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert to float"""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert to int"""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def validate_positive_number(value: float, field_name: str) -> bool:
    """Validate positive number"""
    if value <= 0:
        st.error(f"{field_name} must be greater than 0")
        return False
    return True

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division"""
    try:
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ZeroDivisionError):
        return default

# -------------------- Database helpers --------------------
def execute_query(table: str, operation: str, data: Optional[Dict] = None, 
                 filters: Optional[Dict] = None) -> Tuple[bool, Any]:
    """Generic query executor"""
    try:
        query = supabase.table(table)
        result = None
        
        if operation == "insert":
            if data is None:
                return False, None
            result = query.insert(data).execute()
            
        elif operation == "select":
            query = query.select("*")
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            result = query.execute()
            
        elif operation == "update":
            if data is None:
                return False, None
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
        else:
            return False, None
        
        if result is None:
            return False, None
        return True, result
        
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False, None

# -------------------- Core operations --------------------
def add_stock(fruit: str, boxes: int, cost_per_box: float, dt: Optional[str] = None) -> bool:
    """Add stock with validation"""
    if dt is None:
        dt = date.today().isoformat()
    
    try:
        boxes = int(boxes)
        cost_per_box = float(cost_per_box)
    except (ValueError, TypeError):
        st.error("Invalid numeric values")
        return False
    
    if not fruit or not fruit.strip():
        st.error("Fruit name cannot be empty")
        return False
    
    if boxes <= 0:
        st.error("Boxes must be greater than 0")
        return False
        
    if cost_per_box < 0:
        st.error("Cost cannot be negative")
        return False
    
    data = {
        "fruit": fruit.upper().strip(),
        "quantity": boxes,
        "cost_price": cost_per_box,
        "date": dt,
        "remaining": boxes
    }
    
    success, _ = execute_query("stock", "insert", data)
    if success:
        get_current_stock.clear()
        list_fruits.clear()
    return success

def add_vendor(name: str, contact: str) -> bool:
    """Add vendor with validation"""
    if not name or not name.strip():
        st.error("Vendor name cannot be empty")
        return False
    
    name = name.strip()
    contact = contact.strip()
    
    if not contact.isdigit():
        st.error("Contact must contain only digits")
        return False
        
    if len(contact) != 10:
        st.error("Contact must be exactly 10 digits")
        return False
    
    data = {"name": name, "contact": contact}
    success, _ = execute_query("vendors", "insert", data)
    
    if success:
        list_vendors.clear()
    return success

@st.cache_data(ttl=60)
def list_vendors() -> pd.DataFrame:
    """Get vendors list"""
    try:
        response = supabase.table("vendors").select("*").order("name").execute()
        
        if response is None or not hasattr(response, 'data') or response.data is None:
            return pd.DataFrame(columns=['id', 'name', 'contact'])
        
        if not response.data:
            return pd.DataFrame(columns=['id', 'name', 'contact'])
        
        df = pd.DataFrame(response.data)
        required_cols = ['id', 'name', 'contact']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        return df[required_cols]
        
    except Exception as e:
        st.warning(f"Error loading vendors: {e}")
        return pd.DataFrame(columns=['id', 'name', 'contact'])

@st.cache_data(ttl=60)
def list_fruits() -> List[str]:
    """Get fruits list"""
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        
        if response is None or not hasattr(response, 'data') or not response.data:
            return []
        
        df = pd.DataFrame(response.data)
        
        if 'fruit' not in df.columns or 'remaining' not in df.columns:
            return []
        
        if df.empty:
            return []
        
        df = df[df['remaining'] > 0]
        return sorted(df['fruit'].dropna().unique().tolist())
        
    except Exception as e:
        st.warning(f"Error loading fruits: {e}")
        return []

@st.cache_data(ttl=30)
def get_current_stock() -> Dict[str, int]:
    """Get current stock"""
    try:
        response = supabase.table("stock").select("fruit, remaining").execute()
        
        if response is None or not hasattr(response, 'data') or not response.data:
            return {}
        
        df = pd.DataFrame(response.data)
        
        if 'fruit' not in df.columns or 'remaining' not in df.columns:
            return {}
        
        if df.empty:
            return {}
        
        stock_summary = df.groupby('fruit')['remaining'].sum()
        result = {}
        
        for fruit, qty in stock_summary.items():
            if qty > 0:
                result[fruit] = safe_int(qty, 0)
        
        return result
        
    except Exception as e:
        st.warning(f"Error loading stock: {e}")
        return {}

def reduce_stock_fifo(fruit: str, boxes_to_reduce: int) -> Tuple[bool, str]:
    """Reduce stock using FIFO"""
    try:
        response = supabase.table("stock")\
            .select("*")\
            .eq("fruit", fruit)\
            .gt("remaining", 0)\
            .execute()
        
        if response is None or not hasattr(response, 'data') or not response.data:
            return False, f"No stock found for {fruit}"
        
        stock_entries = response.data
        
        if not stock_entries:
            return False, f"No stock available for {fruit}"
        
        # Python-side sorting for reliability
        stock_entries.sort(key=lambda x: (x.get('date', ''), x.get('id', 0)))
        
        remaining_to_reduce = boxes_to_reduce
        
        for entry in stock_entries:
            if remaining_to_reduce <= 0:
                break
            
            available = safe_int(entry.get('remaining', 0), 0)
            if available <= 0:
                continue
            
            to_reduce = min(available, remaining_to_reduce)
            new_remaining = available - to_reduce
            
            try:
                supabase.table("stock")\
                    .update({"remaining": new_remaining})\
                    .eq("id", entry['id'])\
                    .execute()
            except Exception as e:
                return False, f"Failed to update stock: {e}"
            
            remaining_to_reduce -= to_reduce
        
        if remaining_to_reduce > 0:
            return False, f"Insufficient stock. Short by {remaining_to_reduce} boxes"
        
        return True, "Stock reduced successfully"
        
    except Exception as e:
        return False, f"Error in FIFO reduction: {str(e)}"

def sell_to_vendor(dt: str, vendor_id: int, fruit: str, boxes: int, 
                  price_per_box: float, box_deposit_per_box: float, 
                  note: str = "") -> bool:
    """Record sale"""
    try:
        boxes = int(boxes)
        price_per_box = float(price_per_box)
        box_deposit_per_box = float(box_deposit_per_box)
        vendor_id = int(vendor_id)
    except (ValueError, TypeError):
        st.error("Invalid numeric values")
        return False
    
    if not validate_positive_number(boxes, "Boxes"):
        return False
    
    if price_per_box < 0 or box_deposit_per_box < 0:
        st.error("Price and deposit cannot be negative")
        return False
    
    stock = get_current_stock()
    available = stock.get(fruit, 0)
    
    if boxes > available:
        st.error(f"Insufficient stock. Available: {available} boxes")
        return False
    
    success, message = reduce_stock_fifo(fruit, boxes)
    if not success:
        st.error(message)
        return False
    
    total_price = boxes * price_per_box
    box_deposit_collected = boxes * box_deposit_per_box
    
    sale_data = {
        "dt": dt,
        "vendor_id": vendor_id,
        "fruit": fruit,
        "boxes": boxes,
        "price_per_box": price_per_box,
        "total_price": total_price,
        "box_deposit_per_box": box_deposit_per_box,
        "box_deposit_collected": box_deposit_collected,
        "note": note or ""
    }
    
    success, _ = execute_query("sales", "insert", sale_data)
    
    if success:
        get_current_stock.clear()
        list_fruits.clear()
        vendor_summary_table.clear()
    
    return success

def record_return(dt: str, vendor_id: int, fruit: str, boxes_returned: int, 
                 box_deposit_per_box: float, note: str = "") -> bool:
    """Record return"""
    try:
        boxes_returned = int(boxes_returned)
        box_deposit_per_box = float(box_deposit_per_box)
        vendor_id = int(vendor_id)
    except (ValueError, TypeError):
        st.error("Invalid numeric values")
        return False
    
    if not validate_positive_number(boxes_returned, "Boxes returned"):
        return False
    
    if box_deposit_per_box < 0:
        st.error("Box deposit cannot be negative")
        return False
    
    box_deposit_refunded = boxes_returned * box_deposit_per_box
    
    try:
        return_data = {
            "dt": dt,
            "vendor_id": vendor_id,
            "fruit": fruit,
            "boxes_returned": boxes_returned,
            "box_deposit_refunded": box_deposit_refunded,
            "note": note or ""
        }
        
        supabase.table("returns").insert(return_data).execute()
        
        cost_response = supabase.table("stock")\
            .select("cost_price, quantity")\
            .eq("fruit", fruit)\
            .execute()
        
        avg_cost = 0.0
        
        if cost_response and hasattr(cost_response, 'data') and cost_response.data:
            df = pd.DataFrame(cost_response.data)
            
            if 'cost_price' in df.columns and 'quantity' in df.columns:
                total_qty = df['quantity'].sum()
                if total_qty > 0:
                    total_cost = (df['cost_price'] * df['quantity']).sum()
                    avg_cost = safe_divide(total_cost, total_qty, 0.0)
        
        stock_data = {
            "fruit": fruit,
            "quantity": boxes_returned,
            "cost_price": avg_cost,
            "date": dt,
            "remaining": boxes_returned
        }
        
        supabase.table("stock").insert(stock_data).execute()
        
        get_current_stock.clear()
        vendor_summary_table.clear()
        
        return True
        
    except Exception as e:
        st.error(f"Error recording return: {str(e)}")
        return False

def record_payment(dt: str, vendor_id: int, amount: float, note: str = "") -> bool:
    """Record payment"""
    try:
        amount = float(amount)
        vendor_id = int(vendor_id)
    except (ValueError, TypeError):
        st.error("Invalid numeric values")
        return False
    
    if not validate_positive_number(amount, "Payment amount"):
        return False
    
    payment_data = {
        "dt": dt,
        "vendor_id": vendor_id,
        "amount": amount,
        "note": note or ""
    }
    
    success, _ = execute_query("payments", "insert", payment_data)
    
    if success:
        vendor_summary_table.clear()
    
    return success

# -------------------- Edit Functions --------------------
def get_sales_for_editing(start_date: str, end_date: str) -> pd.DataFrame:
    """Get sales for editing"""
    try:
        response = supabase.table("sales")\
            .select("*")\
            .gte("dt", start_date)\
            .lte("dt", end_date)\
            .order("dt", desc=True)\
            .execute()
        
        if response and hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            
            vendors_df = list_vendors()
            if not vendors_df.empty:
                vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                df['vendor_name'] = df['vendor_id'].map(vendor_map)
            
            return df
        
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading sales: {e}")
        return pd.DataFrame()

def update_sale_record(sale_id: int, updated_data: Dict) -> bool:
    """Update sale record"""
    try:
        boxes = safe_int(updated_data.get('boxes', 0))
        price_per_box = safe_float(updated_data.get('price_per_box', 0.0))
        box_deposit_per_box = safe_float(updated_data.get('box_deposit_per_box', 0.0))
        
        updated_data['total_price'] = boxes * price_per_box
        updated_data['box_deposit_collected'] = boxes * box_deposit_per_box
        
        updated_data.pop('vendor_name', None)
        updated_data.pop('id', None)
        
        success, _ = execute_query("sales", "update", updated_data, {"id": sale_id})
        
        if success:
            vendor_summary_table.clear()
            get_current_stock.clear()
        
        return success
        
    except Exception as e:
        st.error(f"Update failed: {e}")
        return False

def delete_sale_record(sale_id: int) -> bool:
    """Delete sale record"""
    try:
        success, _ = execute_query("sales", "delete", filters={"id": sale_id})
        
        if success:
            vendor_summary_table.clear()
            get_current_stock.clear()
        
        return success
        
    except Exception as e:
        st.error(f"Delete failed: {e}")
        return False

# -------------------- Reporting Functions --------------------
def compute_weighted_avg_cost(fruit: str, up_to_date: Optional[str] = None) -> float:
    """Compute weighted average cost"""
    try:
        query = supabase.table("stock").select("quantity, cost_price").eq("fruit", fruit)
        
        if up_to_date:
            query = query.lte("date", up_to_date)
        
        response = query.execute()
        
        if not response or not hasattr(response, 'data') or not response.data:
            return 0.0
        
        df = pd.DataFrame(response.data)
        
        if df.empty or 'quantity' not in df.columns or 'cost_price' not in df.columns:
            return 0.0
        
        total_boxes = df['quantity'].sum()
        total_cost = (df['quantity'] * df['cost_price']).sum()
        
        return safe_divide(total_cost, total_boxes, 0.0)
        
    except Exception as e:
        st.warning(f"Error computing cost: {e}")
        return 0.0

def compute_cogs_for_sales(sales_df: pd.DataFrame, up_to_date: Optional[str] = None) -> float:
    """Compute COGS"""
    if sales_df is None or sales_df.empty:
        return 0.0
    
    if 'fruit' not in sales_df.columns or 'boxes' not in sales_df.columns:
        return 0.0
    
    cogs = 0.0
    
    try:
        for fruit, grp in sales_df.groupby('fruit'):
            sold_boxes = safe_int(grp['boxes'].sum(), 0)
            avg_cost = compute_weighted_avg_cost(fruit, up_to_date)
            cogs += avg_cost * sold_boxes
    except Exception as e:
        st.warning(f"Error computing COGS: {e}")
    
    return cogs

@st.cache_data(ttl=60)
def vendor_summary_table() -> pd.DataFrame:
    """Generate vendor summary"""
    try:
        vendors_response = supabase.table("vendors").select("id, name").execute()
        
        if not vendors_response or not hasattr(vendors_response, 'data') or not vendors_response.data:
            return pd.DataFrame()
        
        vendors = pd.DataFrame(vendors_response.data)
        
        if vendors.empty or 'id' not in vendors.columns or 'name' not in vendors.columns:
            return pd.DataFrame()
        
        rows = []
        
        for _, v in vendors.iterrows():
            vid = safe_int(v.get('id'), 0)
            vname = str(v.get('name', 'Unknown'))
            
            try:
                sales_response = supabase.table("sales")\
                    .select("fruit, boxes, total_price, box_deposit_collected")\
                    .eq("vendor_id", vid)\
                    .execute()
                
                sales_df = pd.DataFrame(sales_response.data if sales_response and hasattr(sales_response, 'data') and sales_response.data else [])
            except:
                sales_df = pd.DataFrame()
            
            total_sales = safe_float(sales_df['total_price'].sum() if not sales_df.empty and 'total_price' in sales_df.columns else 0, 0.0)
            deposits_collected = safe_float(sales_df['box_deposit_collected'].sum() if not sales_df.empty and 'box_deposit_collected' in sales_df.columns else 0, 0.0)
            
            cogs = compute_cogs_for_sales(sales_df) if not sales_df.empty else 0.0
            profit = total_sales - cogs
            profit_margin = safe_divide(profit * 100, total_sales, 0.0)
            
            try:
                returns_response = supabase.table("returns")\
                    .select("box_deposit_refunded")\
                    .eq("vendor_id", vid)\
                    .execute()
                returns_df = pd.DataFrame(returns_response.data if returns_response and hasattr(returns_response, 'data') and returns_response.data else [])
            except:
                returns_df = pd.DataFrame()
            
            deposits_refunded = safe_float(returns_df['box_deposit_refunded'].sum() if not returns_df.empty and 'box_deposit_refunded' in returns_df.columns else 0, 0.0)
            
            try:
                payments_response = supabase.table("payments")\
                    .select("amount")\
                    .eq("vendor_id", vid)\
                    .execute()
                payments_df = pd.DataFrame(payments_response.data if payments_response and hasattr(payments_response, 'data') and payments_response.data else [])
            except:
                payments_df = pd.DataFrame()
            
            paid = safe_float(payments_df['amount'].sum() if not payments_df.empty and 'amount' in payments_df.columns else 0, 0.0)
            
            net_box_deposits_held = deposits_collected - deposits_refunded
            net_due = total_sales - paid
            
            rows.append({
                "vendor_id": vid,
                "vendor_name": vname,
                "total_sales": total_sales,
                "cogs": cogs,
                "profit": profit,
                "profit_margin": profit_margin,
                "payments": paid,
                "net_due": net_due,
                "deposits_collected": deposits_collected,
                "deposits_refunded": deposits_refunded,
                "net_deposits_held": net_box_deposits_held
            })
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        st.warning(f"Error generating summary: {e}")
        return pd.DataFrame()

def get_daily_summary(selected_date: Optional[date] = None) -> Optional[Dict]:
    """Get daily summary"""
    if selected_date is None:
        selected_date = date.today()
    
    date_str = selected_date.isoformat()
    
    try:
        try:
            sales_response = supabase.table("sales").select("*").eq("dt", date_str).execute()
            sales_data = sales_response.data if sales_response and hasattr(sales_response, 'data') and sales_response.data else []
        except:
            sales_data = []
        
        sales_df = pd.DataFrame(sales_data) if sales_data else pd.DataFrame()
        
        try:
            payments_response = supabase.table("payments").select("*").eq("dt", date_str).execute()
            payments_data = payments_response.data if payments_response and hasattr(payments_response, 'data') and payments_response.data else []
        except:
            payments_data = []
        
        payments_df = pd.DataFrame(payments_data) if payments_data else pd.DataFrame()
        
        try:
            returns_response = supabase.table("returns").select("*").eq("dt", date_str).execute()
            returns_data = returns_response.data if returns_response and hasattr(returns_response, 'data') and returns_response.data else []
        except:
            returns_data = []
        
        returns_df = pd.DataFrame(returns_data) if returns_data else pd.DataFrame()
        
        summary = {
            "date": date_str,
            "total_sales": safe_float(sales_df['total_price'].sum() if not sales_df.empty and 'total_price' in sales_df.columns else 0, 0.0),
            "boxes_sold": safe_int(sales_df['boxes'].sum() if not sales_df.empty and 'boxes' in sales_df.columns else 0, 0),
            "deposits_collected": safe_float(sales_df['box_deposit_collected'].sum() if not sales_df.empty and 'box_deposit_collected' in sales_df.columns else 0, 0.0),
            "payments_received": safe_float(payments_df['amount'].sum() if not payments_df.empty and 'amount' in payments_df.columns else 0, 0.0),
            "boxes_returned": safe_int(returns_df['boxes_returned'].sum() if not returns_df.empty and 'boxes_returned' in returns_df.columns else 0, 0),
            "deposits_refunded": safe_float(returns_df['box_deposit_refunded'].sum() if not returns_df.empty and 'box_deposit_refunded' in returns_df.columns else 0, 0.0),
            "num_transactions": len(sales_df) + len(payments_df) + len(returns_df)
        }
        
        summary['avg_price_per_box'] = safe_divide(summary['total_sales'], summary['boxes_sold'], 0.0)
        
        if not sales_df.empty:
            cogs = compute_cogs_for_sales(sales_df)
            summary['cogs'] = cogs
            summary['profit'] = summary['total_sales'] - cogs
            summary['profit_margin'] = safe_divide(summary['profit'] * 100, summary['total_sales'], 0.0)
        else:
            summary['cogs'] = 0.0
            summary['profit'] = 0.0
            summary['profit_margin'] = 0.0
        
        return summary
        
    except Exception as e:
        st.warning(f"Error generating summary: {e}")
        return None

def vendor_ledger_df(vendor_id: int) -> pd.DataFrame:
    """Generate vendor ledger"""
    try:
        vendor_id = int(vendor_id)
        
        try:
            sales_response = supabase.table("sales")\
                .select("dt, fruit, boxes, total_price, box_deposit_collected, note")\
                .eq("vendor_id", vendor_id)\
                .execute()
            sales_data = sales_response.data if sales_response and hasattr(sales_response, 'data') and sales_response.data else []
        except:
            sales_data = []
        
        sales = pd.DataFrame(sales_data) if sales_data else pd.DataFrame()
        
        if not sales.empty:
            sales['type'] = 'SALE'
            sales['qty'] = sales.get('boxes', 0)
            sales['sale_amount'] = sales.get('total_price', 0.0)
            sales['deposit'] = sales.get('box_deposit_collected', 0.0)
            sales = sales.rename(columns={'dt': 'date'})
            sales = sales[['date', 'type', 'fruit', 'qty', 'sale_amount', 'deposit', 'note']]
        
        try:
            payments_response = supabase.table("payments")\
                .select("dt, amount, note")\
                .eq("vendor_id", vendor_id)\
                .execute()
            payments_data = payments_response.data if payments_response and hasattr(payments_response, 'data') and payments_response.data else []
        except:
            payments_data = []
        
        payments = pd.DataFrame(payments_data) if payments_data else pd.DataFrame()
        
        if not payments.empty:
            payments['type'] = 'PAYMENT'
            payments['fruit'] = None
            payments['qty'] = None
            payments['sale_amount'] = -payments.get('amount', 0.0)
            payments['deposit'] = 0
            payments = payments.rename(columns={'dt': 'date'})
            payments = payments[['date', 'type', 'fruit', 'qty', 'sale_amount', 'deposit', 'note']]
        
        try:
            returns_response = supabase.table("returns")\
                .select("dt, fruit, boxes_returned, box_deposit_refunded, note")\
                .eq("vendor_id", vendor_id)\
                .execute()
            returns_data = returns_response.data if returns_response and hasattr(returns_response, 'data') and returns_response.data else []
        except:
            returns_data = []
        
        returns = pd.DataFrame(returns_data) if returns_data else pd.DataFrame()
        
        if not returns.empty:
            returns['type'] = 'RETURN'
            returns['qty'] = -returns.get('boxes_returned', 0)
            returns['sale_amount'] = 0
            returns['deposit'] = -returns.get('box_deposit_refunded', 0.0)
            returns = returns.rename(columns={'dt': 'date'})
            returns = returns[['date', 'type', 'fruit', 'qty', 'sale_amount', 'deposit', 'note']]
        
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
        
    except Exception as e:
        st.warning(f"Error generating ledger: {e}")
        return pd.DataFrame()

# -------------------- Backup & Export --------------------
def export_all_data() -> Optional[str]:
    """Export all data as JSON"""
    try:
        with st.spinner("Preparing backup..."):
            backup = {
                "export_date": datetime.now().isoformat(),
                "vendors": [],
                "stock": [],
                "sales": [],
                "returns": [],
                "payments": [],
                "rollover_log": []
            }
            
            for table_name in backup.keys():
                if table_name != "export_date":
                    try:
                        response = supabase.table(table_name).select("*").execute()
                        backup[table_name] = response.data if response and hasattr(response, 'data') and response.data else []
                    except Exception as e:
                        st.warning(f"Could not backup {table_name}: {e}")
                        backup[table_name] = []
            
            return json.dumps(backup, indent=2, default=str)
            
    except Exception as e:
        st.error(f"Backup error: {str(e)}")
        return None

def export_to_excel(df: pd.DataFrame) -> BytesIO:
    """Export to Excel"""
    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Report")
        buf.seek(0)
        return buf
    except Exception as e:
        st.error(f"Excel export error: {e}")
        return BytesIO()

def export_to_pdf(df: pd.DataFrame, title: str = "Report") -> BytesIO:
    """Export to PDF"""
    try:
        pdf = FPDF(orientation='L', unit='mm', format='A4')
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=10)
        
        try:
            pdf.set_font("Arial", "B", 14)
        except:
            pdf.set_font("Courier", "B", 14)
        
        safe_title = title.encode('ascii', 'replace').decode('ascii')
        pdf.cell(0, 10, safe_title, ln=1, align='C')
        pdf.ln(4)
        
        def safe_text(x: Any) -> str:
            if pd.isna(x) or x is None:
                return ""
            text = str(x).replace("₹", "Rs.")
            try:
                text.encode('latin-1')
                return text
            except UnicodeEncodeError:
                return text.encode('ascii', 'replace').decode('ascii')
        
        cols = list(df.columns)
        page_width = pdf.w - 2 * pdf.l_margin
        col_w = max(20, page_width / max(1, len(cols)))
        
        try:
            pdf.set_font("Arial", "B", 9)
        except:
            pdf.set_font("Courier", "B", 9)
        
        for col in cols:
            pdf.cell(col_w, 8, safe_text(col), border=1)
        pdf.ln()
        
        try:
            pdf.set_font("Arial", "", 8)
        except:
            pdf.set_font("Courier", "", 8)
        
        for _, row in df.iterrows():
            for item in row:
                text = safe_text(item)
                if len(text) > 40:
                    text = text[:37] + "..."
                pdf.cell(col_w, 7, text, border=1)
            pdf.ln()
            
            if pdf.get_y() > pdf.h - 20:
                pdf.add_page()
                try:
                    pdf.set_font("Arial", "B", 9)
                except:
                    pdf.set_font("Courier", "B", 9)
                for col in cols:
                    pdf.cell(col_w, 8, safe_text(col), border=1)
                pdf.ln()
                try:
                    pdf.set_font("Arial", "", 8)
                except:
                    pdf.set_font("Courier", "", 8)
        
        pdf_output = pdf.output(dest="S")
        if isinstance(pdf_output, str):
            pdf_output = pdf_output.encode('latin-1', 'replace')
        
        buf = BytesIO(pdf_output)
        buf.seek(0)
        return buf
        
    except Exception as e:
        st.error(f"PDF export error: {e}")
        return BytesIO()

def carry_forward_stock() -> bool:
    """Daily rollover"""
    today_str = date.today().isoformat()
    
    try:
        response = supabase.table("rollover_log").select("*").eq("date", today_str).execute()
        
        if response and hasattr(response, 'data') and response.data:
            return False
        
        supabase.table("rollover_log").insert({
            "date": today_str,
            "carried": 1
        }).execute()
        
        return True
        
    except Exception as e:
        st.error(f"Rollover error: {e}")
        return False

# -------------------- STREAMLIT APP --------------------
st.set_page_config(
    page_title="DBF Fruit Manager",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# -------------------- AUTHENTICATION --------------------
config = get_auth_config()

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# Show login form
authenticator.login()

# Get authentication status from session state
authentication_status = st.session_state.get("authentication_status")
name = st.session_state.get("name")
username = st.session_state.get("username")

# Handle authentication results
if authentication_status == False:
    st.error('❌ Username/password is incorrect')
    with st.expander("ℹ️ Help"):
        st.info("""
        **Default credentials:**
        - Username: `admin`
        - Password: `admin123`
        """)
    st.stop()

if authentication_status == None:
    st.warning('⚠️ Please login to continue')
    st.info("""
    **Try these credentials:**
    - Username: `admin`
    - Password: `admin123`
    """)
    st.stop()

# -------------------- AUTHENTICATED APP --------------------
if authentication_status:
    
    st.title("🍎")
    
    # Sidebar
    with st.sidebar:
        st.write(f'👤 **{name}**')
        authenticator.logout()
        st.divider()
        
        # ... rest of your cod
        
        with st.spinner("Loading..."):
            stock = get_current_stock()
            total_boxes = sum(stock.values()) if stock else 0
            st.metric("📦 Stock", f"{total_boxes} boxes")
            
            today_summary = get_daily_summary()
            if today_summary:
                st.metric("💰 Today", f"₹{today_summary['total_sales']:.2f}")
                st.metric("📊 Boxes", today_summary['boxes_sold'])
            
            summary = vendor_summary_table()
            if not summary.empty:
                total_due = summary['net_due'].sum()
                st.metric("💵 Dues", f"₹{total_due:.2f}")
        
        st.divider()
        
        if st.button("🔄 Refresh", width='stretch'):
            st.cache_data.clear()
            st.success("✓")
            st.rerun()
        
        backup_data = export_all_data()
        if backup_data:
            st.download_button(
                "💾 Backup",
                data=backup_data,
                file_name=f"backup_{date.today()}.json",
                mime="application/json",
                width='stretch'
            )
    
    st.title("🍎 DBF Fruit Manager")
    
    # Rollover check
    today_str = date.today().isoformat()
    try:
        rollover_response = supabase.table("rollover_log").select("*").eq("date", today_str).execute()
        if rollover_response and hasattr(rollover_response, 'data') and not rollover_response.data:
            if st.button("🔄 Start New Day", type="primary"):
                with st.spinner("Starting..."):
                    if carry_forward_stock():
                        st.success("✅ New day started!")
                        st.balloons()
                        st.rerun()
    except:
        pass
    
    # Main tabs
    tabs = st.tabs([
        "📋 Vendors",
        "📦 Stock",
        "💰 Sell",
        "↩️ Returns",
        "💵 Payments",
        "✏️ Edit Sales",
        "📊 Dues",
        "📈 Reports",
        "📖 Ledger",
        "📅 Daily"
    ])
    
    # ---------- Tab 0: Vendors ----------
    with tabs[0]:
        st.header("Vendors")
        
        search_vendor = st.text_input("🔍 Search", placeholder="Type name...")
        
        col1, col2 = st.columns([1, 2] if not st.session_state.mobile_view else 1)
        
        with col1:
            st.subheader("Add Vendor")
            with st.form("vendor_form", clear_on_submit=True):
                vname = st.text_input("Name *")
                vcontact = st.text_input("Contact *", max_chars=10)
                
                if st.form_submit_button("➕ Add", type="primary"):
                    with st.spinner("Adding..."):
                        if add_vendor(vname, vcontact):
                            st.success(f"✅ Added '{vname}'!")
                            st.rerun()
        
        with col2:
            st.subheader("All Vendors")
            vendors_df = list_vendors()
            
            if not vendors_df.empty and search_vendor:
                vendors_df = vendors_df[vendors_df['name'].str.contains(search_vendor, case=False, na=False)]
            
            if vendors_df.empty:
                st.info("No vendors")
            else:
                st.dataframe(vendors_df, width='stretch', hide_index=True)
    
    # ---------- Tab 1: Stock ----------
    with tabs[1]:
        st.header("Stock Management")
        
        col1, col2 = st.columns([1, 2] if not st.session_state.mobile_view else 1)
        
        with col1:
            st.subheader("Add Stock")
            with st.form("stock_form", clear_on_submit=True):
                fruit = st.text_input("Fruit *", placeholder="APPLE").upper()
                boxes = st.number_input("Boxes *", min_value=1, value=10)
                cost = st.number_input("Cost/Box (₹) *", min_value=0.0, value=500.0)
                stock_date = st.date_input("Date", value=date.today())
                
                if st.form_submit_button("📦 Add", type="primary"):
                    if fruit.strip():
                        with st.spinner("Adding..."):
                            if add_stock(fruit.strip(), boxes, cost, stock_date.isoformat()):
                                st.success(f"✅ Added {boxes} boxes of {fruit}")
                                st.rerun()
        
        with col2:
            st.subheader("Current Stock")
            with st.spinner("Loading..."):
                stock = get_current_stock()
                if stock:
                    stock_df = pd.DataFrame(list(stock.items()), columns=['Fruit', 'Boxes'])
                    
                    def highlight_low(row):
                        if row['Boxes'] <= 5:
                            return ['background-color: #ffcccc'] * len(row)
                        return [''] * len(row)
                    
                    styled = stock_df.style.apply(highlight_low, axis=1)
                    st.dataframe(styled, width='stretch', hide_index=True)
                    
                    low_stock = {k: v for k, v in stock.items() if v <= 5}
                    if low_stock:
                        st.warning(f"⚠️ Low Stock: {', '.join([f'{k} ({v})' for k, v in low_stock.items()])}")
                else:
                    st.info("No stock")
    
    # ---------- Tab 2: Sell ----------
    with tabs[2]:
        st.header("Record Sale")
        
        st.info("💡 Box deposit is refundable security")
        
        vendors_df = list_vendors()
        fruits = list_fruits()
        
        if vendors_df.empty:
            st.warning("⚠️ Add vendors first")
        elif not fruits:
            st.warning("⚠️ Add stock first")
        else:
            with st.form("sell_form", clear_on_submit=True):
                if st.session_state.mobile_view:
                    sdate = st.date_input("Date *", value=date.today())
                    vendor_choice = st.selectbox("Vendor *", vendors_df['name'].tolist())
                    vendor_id = int(vendors_df[vendors_df['name'] == vendor_choice]['id'].iloc[0])
                    fruit_choice = st.selectbox("Fruit *", fruits)
                    sell_boxes = st.number_input("Boxes *", min_value=1, value=1)
                    current_stock = get_current_stock()
                    st.caption(f"Available: {current_stock.get(fruit_choice, 0)} boxes")
                    price_box = st.number_input("Price/Box (₹) *", min_value=0.0, value=700.0)
                    box_deposit = st.number_input("Deposit/Box (₹) *", min_value=0.0, value=BOX_DEPOSIT_DEFAULT)
                else:
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        sdate = st.date_input("Date *", value=date.today())
                        vendor_choice = st.selectbox("Vendor *", vendors_df['name'].tolist())
                        vendor_id = int(vendors_df[vendors_df['name'] == vendor_choice]['id'].iloc[0])
                    
                    with col2:
                        fruit_choice = st.selectbox("Fruit *", fruits)
                        sell_boxes = st.number_input("Boxes *", min_value=1, value=1)
                        current_stock = get_current_stock()
                        st.caption(f"Available: {current_stock.get(fruit_choice, 0)}")
                    
                    with col3:
                        price_box = st.number_input("Price/Box (₹) *", min_value=0.0, value=700.0)
                        box_deposit = st.number_input("Deposit/Box (₹) *", min_value=0.0, value=BOX_DEPOSIT_DEFAULT)
                
                st.caption(f"💰 Total: ₹{sell_boxes * price_box:.2f} | 📦 Deposit: ₹{sell_boxes * box_deposit:.2f}")
                
                note = st.text_area("Note (optional)")
                
                if st.form_submit_button("💰 Record Sale", type="primary", width='stretch'):
                    with st.spinner("Recording..."):
                        if sell_to_vendor(sdate.isoformat(), vendor_id, fruit_choice, sell_boxes, 
                                         price_box, box_deposit, note):
                            st.success(f"✅ Sale recorded!")
                            st.balloons()
                            st.rerun()
    
    # ---------- Tab 3: Returns ----------
    with tabs[3]:
        st.header("Record Returns")
        
        st.info("💡 Deposit refunded on return")
        
        vendors_df = list_vendors()
        
        if vendors_df.empty:
            st.warning("⚠️ Add vendors first")
        else:
            with st.form("return_form", clear_on_submit=True):
                col1, col2 = st.columns(2 if not st.session_state.mobile_view else 1)
                
                with col1:
                    rdate = st.date_input("Date *", value=date.today())
                    v_choice = st.selectbox("Vendor *", vendors_df['name'].tolist())
                    v_id = int(vendors_df[vendors_df['name'] == v_choice]['id'].iloc[0])
                
                with col2:
                    try:
                        vendor_fruits_response = supabase.table("sales")\
                            .select("fruit")\
                            .eq("vendor_id", v_id)\
                            .execute()
                        vendor_fruits_data = pd.DataFrame(vendor_fruits_response.data if vendor_fruits_response and hasattr(vendor_fruits_response, 'data') and vendor_fruits_response.data else [])
                        
                        if vendor_fruits_data.empty:
                            fruit_r = st.text_input("Fruit", value="APPLE")
                        else:
                            unique_fruits = vendor_fruits_data['fruit'].unique().tolist()
                            fruit_r = st.selectbox("Fruit *", unique_fruits)
                    except:
                        fruit_r = st.text_input("Fruit", value="APPLE")
                    
                    returned_boxes = st.number_input("Boxes *", min_value=1, value=1)
                
                box_deposit = st.number_input("Deposit/Box (₹) *", min_value=0.0, value=BOX_DEPOSIT_DEFAULT)
                st.caption(f"💰 Refund: ₹{returned_boxes * box_deposit:.2f}")
                
                rnote = st.text_area("Note (optional)")
                
                if st.form_submit_button("↩️ Record Return", type="primary", width='stretch'):
                    with st.spinner("Recording..."):
                        if record_return(rdate.isoformat(), v_id, fruit_r, returned_boxes, box_deposit, rnote):
                            st.success(f"✅ Return recorded!")
                            st.rerun()
    
    # ---------- Tab 4: Payments ----------
    with tabs[4]:
        st.header("Record Payments")
        
        st.info("💡 Record fruit payments (NOT deposits)")
        
        vendors_df = list_vendors()
        
        if vendors_df.empty:
            st.warning("⚠️ Add vendors first")
        else:
            col1, col2 = st.columns([1, 2] if not st.session_state.mobile_view else 1)
            
            with col1:
                st.subheader("New Payment")
                
                # Outside form for instant update
                pdate = st.date_input("Date *", value=date.today(), key="pay_date")
                vpay = st.selectbox("Vendor *", vendors_df['name'].tolist(), key="pay_vendor")
                vid = int(vendors_df[vendors_df['name'] == vpay]['id'].iloc[0])
                
                with st.spinner("Loading..."):
                    summary = vendor_summary_table()
                    if not summary.empty:
                        vendor_row = summary[summary['vendor_id'] == vid]
                        if not vendor_row.empty:
                            vendor_due = vendor_row['net_due'].iloc[0]
                            deposits_held = vendor_row['net_deposits_held'].iloc[0]
                            profit_margin = vendor_row['profit_margin'].iloc[0]
                            
                            col_a, col_b = st.columns(2)
                            with col_a:
                                st.metric("Due", f"₹{vendor_due:.2f}")
                            with col_b:
                                st.metric("Deposits", f"₹{deposits_held:.2f}")
                            
                            st.metric("Margin", f"{profit_margin:.1f}%")
                
                st.divider()
                
                with st.form("payment_form", clear_on_submit=True):
                    amount = st.number_input("Amount (₹) *", min_value=0.0, value=0.0, step=100.0)
                    pnote = st.text_area("Note (optional)")
                    
                    if st.form_submit_button("💵 Record", type="primary", width='stretch'):
                        if amount > 0:
                            with st.spinner("Recording..."):
                                if record_payment(pdate.isoformat(), vid, amount, pnote):
                                    st.success(f"✅ Payment recorded!")
                                    st.rerun()
                        else:
                            st.error("Amount must be > 0")
            
            with col2:
                st.subheader("Recent Payments")
                with st.spinner("Loading..."):
                    try:
                        recent_p_response = supabase.table("payments")\
                            .select("dt, vendor_id, amount, note")\
                            .order("dt", desc=True)\
                            .limit(50)\
                            .execute()
                        recent_p = pd.DataFrame(recent_p_response.data if recent_p_response and hasattr(recent_p_response, 'data') and recent_p_response.data else [])
                        
                        if not recent_p.empty:
                            vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                            recent_p['Vendor'] = recent_p['vendor_id'].map(vendor_map)
                            recent_p = recent_p[['dt', 'Vendor', 'amount', 'note']]
                            recent_p.columns = ['Date', 'Vendor', 'Amount', 'Note']
                            recent_p['Amount'] = recent_p['Amount'].apply(lambda x: f"₹{x:.2f}")
                            st.dataframe(recent_p, width='stretch', hide_index=True)
                        else:
                            st.info("No payments yet")
                    except:
                        st.info("No payments yet")
    
    # ---------- Tab 5: Edit Sales ----------
    with tabs[5]:
        st.header("✏️ Edit Sales")
        
        st.warning("⚠️ **Admin Only** - Edit existing sales")
        
        col1, col2 = st.columns(2)
        with col1:
            edit_start = st.date_input("From", value=date.today().replace(day=1), key="edit_start")
        with col2:
            edit_end = st.date_input("To", value=date.today(), key="edit_end")
        
        if st.button("🔍 Load Sales", type="primary"):
            with st.spinner("Loading..."):
                sales_df = get_sales_for_editing(edit_start.isoformat(), edit_end.isoformat())
                
                if not sales_df.empty:
                    st.session_state.edited_sales = sales_df.copy()
                    st.session_state.edit_mode = True
                else:
                    st.warning("No sales found")
        
        if st.session_state.edit_mode and not st.session_state.edited_sales.empty:
            st.subheader("📝 Editable Data")
            
            edit_columns = ['id', 'dt', 'vendor_name', 'fruit', 'boxes', 
                           'price_per_box', 'box_deposit_per_box', 'note']
            
            display_df = st.session_state.edited_sales[edit_columns].copy()
            
            column_config = {
                'id': st.column_config.NumberColumn('ID', disabled=True),
                'dt': st.column_config.TextColumn('Date', disabled=True),  # ✅ FIXED - Changed to TextColumn
                'vendor_name': st.column_config.TextColumn('Vendor', disabled=True),
                'fruit': st.column_config.TextColumn('Fruit'),
                'boxes': st.column_config.NumberColumn('Boxes', min_value=1),
                'price_per_box': st.column_config.NumberColumn('Price/Box', min_value=0.0),
                'box_deposit_per_box': st.column_config.NumberColumn('Deposit/Box', min_value=0.0),
                'note': st.column_config.TextColumn('Note')
                }

            edited_df = st.data_editor(
                display_df,
                column_config=column_config,
                width='stretch',  # ✅ FIXED - Updated deprecated parameter
                num_rows="fixed",
                key="sales_editor",
                hide_index=True
            )

            
            st.divider()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("💾 Save", type="primary", width='stretch'):
                    with st.spinner("Saving..."):
                        changes_made = False
                        
                        for idx in range(len(st.session_state.edited_sales)):
                            orig_row = st.session_state.edited_sales.iloc[idx][edit_columns]
                            edit_row = edited_df.iloc[idx]
                            
                            # Check if changed
                            if not orig_row.equals(edit_row):
                                sale_id = int(edit_row['id'])
                                
                                update_data = {
                                    'dt': str(edit_row['dt']),
                                    'fruit': edit_row['fruit'],
                                    'boxes': int(edit_row['boxes']),
                                    'price_per_box': float(edit_row['price_per_box']),
                                    'box_deposit_per_box': float(edit_row['box_deposit_per_box']),
                                    'note': str(edit_row['note']) if edit_row['note'] else ""
                                }
                                
                                if update_sale_record(sale_id, update_data):
                                    changes_made = True
                                else:
                                    st.error(f"Failed to update ID: {sale_id}")
                        
                        if changes_made:
                            st.success("✅ Changes saved!")
                            st.session_state.edit_mode = False
                            st.session_state.edited_sales = pd.DataFrame()
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.info("No changes detected")
            
            with col2:
                if st.button("❌ Cancel", width='stretch'):
                    st.session_state.edit_mode = False
                    st.session_state.edited_sales = pd.DataFrame()
                    st.rerun()
            
            with col3:
                excel_buf = export_to_excel(edited_df)
                st.download_button(
                    "📥 Export",
                    data=excel_buf,
                    file_name=f"sales_edit_{date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch'
                )
            
            with col4:
                if st.button("🗑️ Delete Mode", width='stretch'):
                    st.session_state.delete_mode = not st.session_state.delete_mode
            
            if st.session_state.delete_mode:
                st.warning("⚠️ **DELETE MODE** - Enter ID to delete")
                
                delete_id = st.number_input("Sale ID", min_value=1, step=1)
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🗑️ Confirm Delete", type="primary"):
                        if delete_sale_record(delete_id):
                            st.success(f"✅ Deleted ID {delete_id}")
                            st.session_state.edit_mode = False
                            st.session_state.edited_sales = pd.DataFrame()
                            st.session_state.delete_mode = False
                            st.rerun()
                
                with col2:
                    if st.button("❌ Cancel Delete"):
                        st.session_state.delete_mode = False
                        st.rerun()
            
            with st.expander("📊 Summary"):
                st.metric("Records", len(edited_df))
                st.metric("Range", f"{edit_start} to {edit_end}")
        
        else:
            st.info("👆 Select dates and click 'Load Sales'")
    
    # ---------- Tab 6: Dues ----------
    with tabs[6]:
        st.header("Vendor Dues")
        
        search_dues = st.text_input("🔍 Search", key="search_dues")
        
        with st.spinner("Loading..."):
            summary = vendor_summary_table()
        
        if summary.empty:
            st.info("No transactions")
        else:
            if search_dues:
                summary = summary[summary['vendor_name'].str.contains(search_dues, case=False, na=False)]
            
            if summary.empty:
                st.info("No matches")
            else:
                if not st.session_state.mobile_view:
                    col1, col2, col3, col4, col5 = st.columns(5)
                else:
                    col1, col2 = st.columns(2)
                    col3, col4, col5 = st.columns(3)
                
                col1.metric("Sales", f"₹{summary['total_sales'].sum():.2f}")
                col2.metric("Paid", f"₹{summary['payments'].sum():.2f}")
                col3.metric("Due", f"₹{summary['net_due'].sum():.2f}")
                col4.metric("Deposits", f"₹{summary['net_deposits_held'].sum():.2f}")
                col5.metric("Profit", f"₹{summary['profit'].sum():.2f}")
                
                st.divider()
                
                summary_display = summary[['vendor_name','total_sales','cogs','profit','profit_margin',
                                          'payments','net_due','net_deposits_held']].copy()
                summary_display.columns = ['Vendor', 'Sales', 'COGS', 'Profit', 'Margin%',
                                          'Paid', 'Due', 'Deposits']
                
                for col in ['Sales', 'COGS', 'Profit', 'Paid', 'Due', 'Deposits']:
                    summary_display[col] = summary_display[col].apply(lambda x: f"₹{x:.2f}")
                
                summary_display['Margin%'] = summary_display['Margin%'].apply(lambda x: f"{x:.1f}%")
                
                st.dataframe(summary_display, width='stretch', hide_index=True)
                
                st.divider()
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    excel_buf = export_to_excel(summary)
                    st.download_button(
                        "📥 Excel",
                        data=excel_buf,
                        file_name=f"Dues_{date.today()}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch'
                    )
                
                with col2:
                    pdf_buf = export_to_pdf(summary, "Vendor Dues")
                    st.download_button(
                        "📥 PDF",
                        data=pdf_buf,
                        file_name=f"Dues_{date.today()}.pdf",
                        mime="application/pdf",
                        width='stretch'
                    )
    
    # ---------- Tab 7: Reports ----------
    with tabs[7]:
        st.header("Sales Reports")
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("From", value=date.today().replace(day=1))
        with col2:
            end_date = st.date_input("To", value=date.today())
        
        with st.spinner("Generating..."):
            try:
                sales_response = supabase.table("sales")\
                    .select("dt, vendor_id, fruit, boxes, price_per_box, total_price, box_deposit_collected")\
                    .gte("dt", start_date.isoformat())\
                    .lte("dt", end_date.isoformat())\
                    .order("dt", desc=True)\
                    .execute()
                
                sales_df = pd.DataFrame(sales_response.data if sales_response and hasattr(sales_response, 'data') and sales_response.data else [])
                
                if sales_df.empty:
                    st.info("No sales in range")
                else:
                    vendors_df = list_vendors()
                    vendor_map = dict(zip(vendors_df['id'], vendors_df['name']))
                    sales_df['Vendor'] = sales_df['vendor_id'].map(vendor_map)
                    
                    total_revenue = float(sales_df['total_price'].sum())
                    cogs = compute_cogs_for_sales(sales_df)
                    pnl = total_revenue - cogs
                    
                    if not st.session_state.mobile_view:
                        col1, col2, col3, col4 = st.columns(4)
                    else:
                        col1, col2 = st.columns(2)
                        col3, col4 = st.columns(2)
                    
                    col1.metric("Revenue", f"₹{total_revenue:.2f}")
                    col2.metric("COGS", f"₹{cogs:.2f}")
                    col3.metric("Profit", f"₹{pnl:.2f}")
                    col4.metric("Margin", f"{safe_divide(pnl*100, cogs, 0):.1f}%")
                    
                    st.divider()
                    
                    sales_display = sales_df[['dt', 'Vendor', 'fruit', 'boxes', 'price_per_box', 'total_price']].copy()
                    sales_display.columns = ['Date', 'Vendor', 'Fruit', 'Boxes', 'Price', 'Total']
                    
                    for col in ['Price', 'Total']:
                        sales_display[col] = sales_display[col].apply(lambda x: f"₹{x:.2f}")
                    
                    st.dataframe(sales_display, width='stretch', hide_index=True)
                    
                    st.divider()
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        excel_buf = export_to_excel(sales_df)
                        st.download_button(
                            "📥 Excel",
                            data=excel_buf,
                            file_name=f"Sales_{start_date}_{end_date}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            width='stretch'
                        )
                    
                    with col2:
                        pdf_buf = export_to_pdf(sales_df, "Sales Report")
                        st.download_button(
                            "📥 PDF",
                            data=pdf_buf,
                            file_name=f"Sales_{start_date}_{end_date}.pdf",
                            mime="application/pdf",
                            width='stretch'
                        )
            except:
                st.info("No sales")
    
    # ---------- Tab 8: Ledger ----------
    with tabs[8]:
        st.header("Vendor Ledger")
        
        vendors_df = list_vendors()
        
        if vendors_df.empty:
            st.warning("⚠️ Add vendors")
        else:
            vchoice = st.selectbox("Vendor", vendors_df['name'].tolist(), key="ledger_vendor")
            vid = int(vendors_df[vendors_df['name'] == vchoice]['id'].iloc[0])
            
            with st.spinner("Loading..."):
                ledger = vendor_ledger_df(vid)
            
            if ledger.empty:
                st.info(f"No transactions for {vchoice}")
            else:
                final_due = ledger['running_due'].iloc[-1]
                final_deposits = ledger['running_deposits'].iloc[-1]
                
                col1, col2 = st.columns(2)
                col1.metric("Due", f"₹{final_due:.2f}")
                col2.metric("Deposits", f"₹{final_deposits:.2f}")
                
                st.divider()
                
                ledger_display = ledger.copy()
                ledger_display['sale_amount'] = ledger_display['sale_amount'].apply(lambda x: f"₹{x:.2f}")
                ledger_display['deposit'] = ledger_display['deposit'].apply(lambda x: f"₹{x:.2f}")
                ledger_display['running_due'] = ledger_display['running_due'].apply(lambda x: f"₹{x:.2f}")
                ledger_display['running_deposits'] = ledger_display['running_deposits'].apply(lambda x: f"₹{x:.2f}")
                
                ledger_display.columns = ['Date', 'Type', 'Fruit', 'Qty', 'Amount', 'Deposit', 
                                         'Note', 'Running Due', 'Running Deposits']
                
                st.dataframe(ledger_display, width='stretch', hide_index=True)
                
                st.divider()
                col1, col2 = st.columns(2)
                
                with col1:
                    excel_buf = export_to_excel(ledger)
                    st.download_button(
                        "📥 Excel",
                        data=excel_buf,
                        file_name=f"{vchoice}_Ledger_{date.today()}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch'
                    )
                
                with col2:
                    pdf_buf = export_to_pdf(ledger, f"{vchoice} Ledger")
                    st.download_button(
                        "📥 PDF",
                        data=pdf_buf,
                        file_name=f"{vchoice}_Ledger_{date.today()}.pdf",
                        mime="application/pdf",
                        width='stretch'
                    )
    
    # ---------- Tab 9: Daily Summary ----------
    with tabs[9]:
        st.header("📅 Daily Summary")
        
        selected_date = st.date_input("Date", value=date.today(), key="summary_date")
        
        with st.spinner("Loading..."):
            summary = get_daily_summary(selected_date)
        
        if summary and summary['num_transactions'] > 0:
            st.subheader(f"Summary for {summary['date']}")
            
            if not st.session_state.mobile_view:
                col1, col2, col3, col4 = st.columns(4)
            else:
                col1, col2 = st.columns(2)
                col3, col4 = st.columns(2)
            
            col1.metric("💰 Sales", f"₹{summary['total_sales']:.2f}")
            col2.metric("📦 Boxes", summary['boxes_sold'])
            col3.metric("💵 Payments", f"₹{summary['payments_received']:.2f}")
            col4.metric("↩️ Returns", summary['boxes_returned'])
            
            st.divider()
            
            if not st.session_state.mobile_view:
                col1, col2, col3, col4 = st.columns(4)
            else:
                col1, col2 = st.columns(2)
                col3, col4 = st.columns(2)
            
            col1.metric("Avg/Box", f"₹{summary['avg_price_per_box']:.2f}")
            col2.metric("COGS", f"₹{summary['cogs']:.2f}")
            col3.metric("Profit", f"₹{summary['profit']:.2f}")
            col4.metric("Margin", f"{summary['profit_margin']:.1f}%")
            
            st.divider()
            
            col1, col2 = st.columns(2)
            col1.metric("Deposits In", f"₹{summary['deposits_collected']:.2f}")
            col2.metric("Deposits Out", f"₹{summary['deposits_refunded']:.2f}")
            
            st.success(f"""
            **Summary:**  
            Transactions: {summary['num_transactions']} | Cash: ₹{summary['payments_received']:.2f} | Profit: ₹{summary['profit']:.2f}
            """)
            
        else:
            st.info(f"No transactions on {selected_date}")
    
    # Footer
    st.divider()
    st.caption(f"🍎 DBF Fruit Manager v5.0 - User: {name}")
    st.caption("Features: Secure Login ✓ | Edit Sales ✓ | Full Analytics ✓ | Mobile Responsive ✓")








