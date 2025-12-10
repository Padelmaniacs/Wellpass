import streamlit as st
import pandas as pd
from rapidfuzz import fuzz, process
from datetime import datetime, date, timedelta
import io
import gspread
from google.oauth2.service_account import Credentials
import time
import numpy as np
import plotly.graph_objects as go
from calendar import monthrange
import random
import hashlib
import re


# ========================================
# KONSTANTEN & KONFIGURATION
# ========================================

WELLPASS_WERT = 13.50
WELLPASS_QR_LINK = "https://cdn.jsdelivr.net/gh/PadelPort/PP/Wellpass.jpeg"

PRODUCT_TYPES = {
    'User booking registration': 'Reservierung',
    'Open match registration': 'Open Match',
    'Product extra - BALLS': 'Bälle',
    'Product extra - RACKET': 'Schläger',
}

MITARBEITER = {
    'Bella Schopf', 'Chris Schopf', 'Dagmar Ludwig', 'Denis Messerschmidt', 'Dennis Ochmann', 
    'Enna Kulasevic', 'Fabio Pfaffenbauer', 'Lena Wagenblast', 'Lewis Abraham', 'Lisa Pfaffenbauer', 
    'Manuel Müller', 'Michaela Schopf', 'Nico Warga', 'Noemi Mantel', 
    'Oliver Krieger', 'Pascal Menikheim', 'Tobi Regnet', 'Valentin Schwamborn',
    'Spieler 1', 'Spieler 2', 'Spieler 3', 'Spieler 4', 'Playtomic'
}

# ========================================
# HELPER FUNCTIONS
# ========================================

def get_wellpass_wert(for_date: date) -> float:
    """
    Gibt den passenden Wellpass-Payout für ein Datum zurück.
    Ab 01.12.2025: 13.50 €, davor 12.50 €.
    """
    grenze = date(2025, 12, 1)
    if for_date >= grenze:
        return 13.50
    return 12.50

def send_wellpass_template(to_phone_e164: str, firstname: str, playtime: str) -> bool:
    """
    Sendet das Twilio WhatsApp-Template:
    🎾 Hey {{1}}! ... Du hast um {{2}} Uhr gespielt. ... {{3}} = QR-Link
    """
    try:
        from twilio.rest import Client

        twilio_conf = st.secrets.get("twilio", {})
        account_sid = twilio_conf.get("account_sid")
        auth_token = twilio_conf.get("auth_token")
        from_number = twilio_conf.get("whatsapp_from")  # z.B. 'whatsapp:+14155238886'
        template_name = twilio_conf.get("template_name", "wellpass_reminder_v1")

        if not all([account_sid, auth_token, from_number]):
            st.error("❌ Twilio-Konfiguration unvollständig (account_sid/auth_token/whatsapp_from).")
            return False

        client = Client(account_sid, auth_token)

        # Twilio-konforme Nummer
        if not to_phone_e164.startswith("+"):
            # Fallback: deutsche Nummer annehmen
            to_phone_e164 = "+49" + to_phone_e164.lstrip("0").replace(" ", "")
        to_number = f"whatsapp:{to_phone_e164}"

        message = client.messages.create(
            from_=from_number,
            to=to_number,
            content_sid=None,  # falls du Content Templates nutzt, sonst weglassen
            # Für klassische Template-API (WhatsApp Business Templates):
            # Twilio Doku: du kannst 'body' + 'template' je nach Setup nutzen.
            body=None,
            provide_feedback=False,
            # Neuere API: messaging_service_sid + content variables – hier minimal generisch:
            # Wir nutzen die 'template' Struktur, wie in deiner bisherigen send_fehler_notification_with_link-Funktion:
            # (falls du die schon im Code hast, kannst du sie anpassen statt neu zu bauen)
        )

        st.success(f"✅ WhatsApp-Template gesendet! SID: {message.sid}")
        return True

    except Exception as e:
        st.error(f"❌ WhatsApp-Fehler: {e}")
        return False

def send_wellpass_whatsapp_to_player(fehler_row: pd.Series) -> bool:
    """
    Sendet eine WhatsApp-Template-Nachricht (Wellpass-Reminder)
    direkt an den Spieler (Nummer aus dem Customers-Sheet).
    Nutzt das Twilio Content Template mit Platzhaltern:
    {{1}} = Vorname, {{2}} = Spielzeit, {{3}} = QR-Link, {{4}} = Datum.
    """
    try:
        from twilio.rest import Client
        import json

        twilio_conf = st.secrets.get("twilio", {})
        account_sid = twilio_conf.get("account_sid")
        auth_token = twilio_conf.get("auth_token")
        from_number = twilio_conf.get("whatsapp_from")  # z.B. 'whatsapp:+14155238886'
        content_sid = twilio_conf.get("content_sid", "HXe817b0a8d139ff7fcc7e5e476989bcb9")

        if not all([account_sid, auth_token, from_number, content_sid]):
            st.error("❌ Twilio-Konfiguration unvollständig (account_sid/auth_token/whatsapp_from/content_sid).")
            return False

        # 1) Telefonnummer aus Customers-Sheet holen
        customers = loadsheet("customers")
        if customers.empty or "name" not in customers.columns:
            st.error("❌ Customers-Sheet leer oder 'name'-Spalte fehlt.")
            return False

        player_name_norm = normalize_name(fehler_row["Name"])
        match = customers[customers["name"].apply(normalize_name) == player_name_norm]

        if match.empty or "phone_number" not in match.columns or pd.isna(match.iloc[0]["phone_number"]):
            st.error(f"❌ Keine Telefonnummer für {fehler_row['Name']} im Customers-Sheet gefunden.")
            return False

        raw_phone = str(match.iloc[0]["phone_number"]).strip()
        if not raw_phone:
            st.error(f"❌ Leere Telefonnummer für {fehler_row['Name']}.")
            return False

        # Nummer säubern
        raw_phone = raw_phone.replace(" ", "").replace("-", "")

        # WhatsApp-Nummer normalisieren
        if raw_phone.startswith("whatsapp:"):
            to_number = raw_phone
        else:
            if raw_phone.startswith("+"):
                e164 = raw_phone
            else:
                e164 = "+49" + raw_phone.lstrip("0")
            to_number = f"whatsapp:{e164}"

        # Template-Variablen
        full_name = str(fehler_row.get("Name", "")).strip()
        firstname = full_name.split()[0] if full_name else "Padel-Fan"

        # Zeit ({{2}})
        spielzeit = str(fehler_row.get("Service_Zeit", "") or "").strip()
        if not spielzeit:
            spielzeit = "deiner gebuchten Zeit"

        # Datum ({{4}})
        service_date = fehler_row.get("Datum", "")
        if pd.isna(service_date) or service_date == "":
            service_date_str = ""
        else:
            try:
                service_date_str = pd.to_datetime(service_date).strftime("%d.%m.%Y")
            except Exception:
                service_date_str = str(service_date)

        # QR-Link ({{3}})
        qr_link = WELLPASS_QR_LINK

        client = Client(account_sid, auth_token)

        msg = client.messages.create(
            from_=from_number,
            to=to_number,
            content_sid=content_sid,
            content_variables=json.dumps({
                "1": firstname,
                "2": spielzeit,
                "3": qr_link,
                "4": service_date_str,
            }),
        )

        st.success(f"✅ WhatsApp-Template an {full_name} gesendet (SID: {msg.sid})")
        log_whatsapp_sent(fehler_row, to_number)
        return True

    except Exception as e:
        st.error(f"❌ WhatsApp-Fehler: {e}")
        return False


def send_wellpass_whatsapp_test(fehler_row: pd.Series) -> bool:
    """
    Sendet die Wellpass-Reminder-Template-Nachricht als TEST an die Admin-Nummer
    (twilio.whatsapp_to in st.secrets), nicht an den Spieler.
    Nutzt dasselbe Content Template ({{1}}, {{2}}, {{3}}, {{4}}).
    """
    try:
        from twilio.rest import Client
        import json

        twilio_conf = st.secrets.get("twilio", {})
        account_sid = twilio_conf.get("account_sid")
        auth_token = twilio_conf.get("auth_token")
        from_number = twilio_conf.get("whatsapp_from")
        admin_phone = twilio_conf.get("whatsapp_to")
        content_sid = twilio_conf.get("content_sid", "HXe817b0a8d139ff7fcc7e5e476989bcb9")

        if not all([account_sid, auth_token, from_number, admin_phone, content_sid]):
            st.error("❌ Twilio-Konfiguration unvollständig (account_sid/auth_token/whatsapp_from/whatsapp_to/content_sid).")
            return False

        raw_phone = str(admin_phone).strip()
        if not raw_phone:
            st.error("❌ Admin-Nummer (twilio.whatsapp_to) ist leer.")
            return False

        raw_phone = raw_phone.replace(" ", "").replace("-", "")

        if raw_phone.startswith("whatsapp:"):
            to_number = raw_phone
        else:
            if raw_phone.startswith("+"):
                e164 = raw_phone
            else:
                e164 = "+49" + raw_phone.lstrip("0")
            to_number = f"whatsapp:{e164}"

        full_name = str(fehler_row.get("Name", "")).strip()
        firstname = full_name.split()[0] if full_name else "Padel-Fan"

        spielzeit = str(fehler_row.get("Service_Zeit", "") or "").strip()
        if not spielzeit:
            spielzeit = "deiner gebuchten Zeit"

        service_date = fehler_row.get("Datum", "")
        if pd.isna(service_date) or service_date == "":
            service_date_str = ""
        else:
            try:
                service_date_str = pd.to_datetime(service_date).strftime("%d.%m.%Y")
            except Exception:
                service_date_str = str(service_date)

        qr_link = WELLPASS_WERT

        client = Client(account_sid, auth_token)

        msg = client.messages.create(
            from_=from_number,
            to=to_number,
            content_sid=content_sid,
            content_variables=json.dumps({
                "1": firstname + " (TEST)",
                "2": spielzeit,
                "3": qr_link,
                "4": service_date_str,
            }),
        )

        st.success(f"✅ Test-Template an Admin gesendet (SID: {msg.sid})")
        return True

    except Exception as e:
        st.error(f"❌ WhatsApp-Fehler (Test): {e}")
        return False




def send_wellpass_whatsapp_test(fehler_row: pd.Series) -> bool:
    """
    Sendet die Wellpass-Reminder-Template-Nachricht als TEST an die Admin-Nummer
    (twilio.whatsapp_to in st.secrets), nicht an den Spieler.
    Nutzt dasselbe Content Template ({{1}}, {{2}}, {{3}}).
    """
    try:
        from twilio.rest import Client
        import json

        twilio_conf = st.secrets.get("twilio", {})
        account_sid = twilio_conf.get("account_sid")
        auth_token = twilio_conf.get("auth_token")
        from_number = twilio_conf.get("whatsapp_from")
        admin_phone = twilio_conf.get("whatsapp_to")
        content_sid = twilio_conf.get("content_sid", "HXe817b0a8d139ff7fcc7e5e476989bcb9")

        if not all([account_sid, auth_token, from_number, admin_phone, content_sid]):
            st.error("❌ Twilio-Konfiguration unvollständig (account_sid/auth_token/whatsapp_from/whatsapp_to/content_sid).")
            return False

        raw_phone = str(admin_phone).strip()
        if not raw_phone:
            st.error("❌ Admin-Nummer (twilio.whatsapp_to) ist leer.")
            return False

        raw_phone = raw_phone.replace(" ", "").replace("-", "")

        if raw_phone.startswith("whatsapp:"):
            to_number = raw_phone
        else:
            if raw_phone.startswith("+"):
                e164 = raw_phone
            else:
                e164 = "+49" + raw_phone.lstrip("0")
            to_number = f"whatsapp:{e164}"
        full_name = str(fehler_row.get("Name", "")).strip()
        firstname = full_name.split()[0] if full_name else "Padel-Fan"

        spielzeit = str(fehler_row.get("Service_Zeit", "") or "").strip()
        if not spielzeit:
            spielzeit = "deiner gebuchten Zeit"

        service_date = fehler_row.get("Datum", "")
        if pd.isna(service_date) or service_date == "":
            service_date_str = ""
        else:
            try:
                service_date_str = pd.to_datetime(service_date).strftime("%d.%m.%Y")
            except Exception:
                service_date_str = str(service_date)

        qr_link = WELLPASS_QR_LINK

        client = Client(account_sid, auth_token)

        msg = client.messages.create(
            from_=from_number,
            to=to_number,
            content_sid=content_sid,
            content_variables=json.dumps({
                "1": firstname + " (TEST)",
                "2": spielzeit,
                "3": qr_link,
                "4": service_date_str,
            }),
        )


        st.success(f"✅ Test-Template an Admin gesendet (SID: {msg.sid})")
        return True

    except Exception as e:
        st.error(f"❌ WhatsApp-Fehler (Test): {e}")
        return False


    
def validate_secrets():
    required = ["gcp_service_account", "google_sheets", "passwords"]
    missing = [k for k in required if k not in st.secrets]
    if missing:
        st.error(f"❌ Fehlende Secrets: {', '.join(missing)}")
        st.stop()

def normalize_name(name):
    if pd.isna(name):
        return ''
    return (str(name).strip().lower()
            .replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
            .replace('ß', 'ss').replace('-', ' ').replace('  ', ' '))

def parse_date_safe(date_val):
    if pd.isna(date_val) or date_val == '':
        return None
    
    date_str = str(date_val).strip()
    formats = [
        '%d/%m/%Y %H:%M', '%d/%m/%Y', 
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
        '%d.%m.%Y', '%Y%m%d'
    ]
    
    for fmt in formats:
        try:
            return pd.to_datetime(date_str, format=fmt, errors='raise').date()
        except:
            continue
    
    try:
        return pd.to_datetime(date_str, dayfirst=True, errors='raise').date()
    except:
        return None

def parse_csv(f):
    try:
        content = f.read()
        f.seek(0)
        
        text = None
        for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try:
                text = content.decode(encoding)
                break
            except:
                continue
        
        if text is None:
            return pd.DataFrame()
        
        sample = text[:2000]
        semicolon_count = sample.count(';')
        comma_count = sample.count(',')
        tab_count = sample.count('\t')
        
        if semicolon_count > comma_count and semicolon_count > tab_count:
            delimiter = ';'
        elif tab_count > comma_count and tab_count > semicolon_count:
            delimiter = '\t'
        else:
            delimiter = ','
        
        df = pd.read_csv(
            io.StringIO(text), 
            sep=delimiter,
            engine='python',
            on_bad_lines='skip',
            encoding_errors='ignore'
        )
        
        if len(df.columns) > 1:
            return df
        
    except:
        pass
    
    for sep in [None, ';', ',', '\t']:
        try:
            f.seek(0)
            df = pd.read_csv(f, sep=sep, engine='python', encoding='utf-8-sig', on_bad_lines='skip')
            if len(df.columns) > 1:
                return df
        except:
            continue
    
    return pd.DataFrame()

def color_fehler(val):
    if val == 'Ja':
        return 'background-color: #ffcccc; color: #cc0000; font-weight: bold'
    elif val == 'Nein':
        return 'background-color: #ccffcc; color: #006600; font-weight: bold'
    return ''

def optimize_dataframe(df):
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif col_type == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif col_type == 'object':
            num_unique = df[col].nunique()
            num_total = len(df[col])
            if num_unique / num_total < 0.5:
                df[col] = df[col].astype('category')
    
    return df

# ========================================
# ✅ VERBESSERTES AUTHENTICATION MIT COOKIES
# ========================================

def get_cookie_hash():
    """Generiere eindeutigen Browser-Hash"""
    if 'browser_id' not in st.session_state:
        import platform
        browser_fingerprint = f"{platform.system()}_{platform.node()}_{time.time()}"
        st.session_state.browser_id = hashlib.md5(browser_fingerprint.encode()).hexdigest()
    return st.session_state.browser_id

def save_auth_cookie():
    """Speichere Auth-Status mit Timestamp"""
    cookie_hash = get_cookie_hash()
    
    try:
        auth_data = loadsheet("auth_cookies", ['cookie_hash', 'timestamp', 'expires'])
        
        now = datetime.now()
        if not auth_data.empty:
            auth_data['expires'] = pd.to_datetime(auth_data['expires'], errors='coerce')
            auth_data = auth_data[auth_data['expires'] > now]
        
        expires = now + timedelta(days=30)
        new_cookie = pd.DataFrame([{
            'cookie_hash': cookie_hash,
            'timestamp': now.isoformat(),
            'expires': expires.isoformat()
        }])
        
        auth_data = pd.concat([auth_data, new_cookie], ignore_index=True)
        auth_data = auth_data.drop_duplicates(subset=['cookie_hash'], keep='last')
        
        savesheet(auth_data, "auth_cookies")
        
        st.session_state['auth_token'] = cookie_hash
        st.session_state['auth_timestamp'] = time.time()
        
        return True
    except:
        st.session_state['auth_token'] = cookie_hash
        st.session_state['auth_timestamp'] = time.time()
        return False

def check_auth_cookie():
    """Prüfe ob gültiger Cookie existiert"""
    cookie_hash = get_cookie_hash()
    
    if 'auth_token' in st.session_state and st.session_state['auth_token'] == cookie_hash:
        if time.time() - st.session_state.get('auth_timestamp', 0) < 30 * 24 * 60 * 60:
            return True
    
    try:
        auth_data = loadsheet("auth_cookies", ['cookie_hash', 'expires'])
        
        if not auth_data.empty:
            auth_data['expires'] = pd.to_datetime(auth_data['expires'], errors='coerce')
            
            match = auth_data[auth_data['cookie_hash'] == cookie_hash]
            
            if not match.empty:
                expires = match.iloc[0]['expires']
                
                if pd.notna(expires) and expires > datetime.now():
                    st.session_state['auth_token'] = cookie_hash
                    st.session_state['auth_timestamp'] = time.time()
                    return True
    except:
        pass
    
    return False

def check_password():
    """Haupt-Login-Funktion mit Cookie-Support"""
    if check_auth_cookie():
        return True
    
    def entered():
        password = st.session_state.get("password", "")
        correct_password = st.secrets.get("passwords", {}).get("admin_password", "")
        
        if password and password == correct_password:
            save_auth_cookie()
            st.session_state["password_correct"] = True
            if "password" in st.session_state:
                del st.session_state["password"]
        elif password:
            st.session_state["password_correct"] = False
    
    if st.session_state.get("password_correct", False):
        return True
    
    st.markdown("<h1 style='text-align: center;'>🎾 Padel Port Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: #2c3e50;'>🔒 Anmelden</h3>", unsafe_allow_html=True)
    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.text_input("🔑 Passwort:", type="password", on_change=entered, key="password")
        
        if st.session_state.get("password_correct") == False:
            st.error("😕 Falsches Passwort!")
        
        st.success("🍪 Passwort wird 30 Tage gespeichert!")
        st.caption("💡 Du musst dich nur noch 1x pro Monat anmelden")
    
    return False


# ========================================
# WHATSAPP INTEGRATION
# ========================================

def send_whatsapp_message(to_number, message_text):
    try:
        from twilio.rest import Client
        
        account_sid = st.secrets.get("twilio", {}).get("account_sid")
        auth_token = st.secrets.get("twilio", {}).get("auth_token")
        from_number = st.secrets.get("twilio", {}).get("whatsapp_from")
        
        if not all([account_sid, auth_token, from_number]):
            st.error("❌ Twilio nicht konfiguriert")
            return False
        
        client = Client(account_sid, auth_token)
        
        message = client.messages.create(
            from_=from_number,
            body=message_text,
            to=to_number
        )
        
        st.success(f"✅ WhatsApp gesendet! SID: {message.sid}")
        return True
        
    except Exception as e:
        st.error(f"❌ WhatsApp-Fehler: {e}")
        return False

def get_whatsapp_log_key(fehler_row):
    return f"{fehler_row['Name_norm']}_{fehler_row['Datum']}_{fehler_row['Betrag']}"

def log_whatsapp_sent(fehler_row, to_number):
    log = loadsheet("whatsapp_log", cols=['key', 'name', 'datum', 'betrag', 'to_number', 'timestamp'])
    
    key = get_whatsapp_log_key(fehler_row)
    
    if not log.empty and 'key' in log.columns:
        existing = log[log['key'] == key]
        if not existing.empty:
            log.loc[log['key'] == key, 'timestamp'] = datetime.now().isoformat()
            savesheet(log, "whatsapp_log")
            return
    
    new_row = pd.DataFrame([{
        'key': key,
        'name': fehler_row['Name'],
        'datum': fehler_row['Datum'],
        'betrag': fehler_row['Betrag'],
        'to_number': to_number,
        'timestamp': datetime.now().isoformat()
    }])
    
    log = pd.concat([log, new_row], ignore_index=True)
    savesheet(log, "whatsapp_log")

def get_whatsapp_sent_time(fehler_row):
    log = loadsheet("whatsapp_log", cols=['key', 'timestamp'])
    
    if log.empty or 'key' not in log.columns:
        return None
    
    key = get_whatsapp_log_key(fehler_row)
    match = log[log['key'] == key]
    
    if not match.empty:
        timestamp_str = match.iloc[0]['timestamp']
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            return timestamp
        except:
            return None
    
    return None

def send_fehler_notification_with_link(fehler_row, to_player=False):
    if to_player:
        customers = loadsheet("customers")
        if not customers.empty and 'name' in customers.columns:
            player_name_norm = normalize_name(fehler_row['Name'])
            match = customers[customers['name'].apply(normalize_name) == player_name_norm]
            
            if not match.empty and 'phone_number' in match.columns and pd.notna(match.iloc[0]['phone_number']):
                phone = str(match.iloc[0]['phone_number'])
                if not phone.startswith('+'):
                    phone = '+49' + phone.lstrip('0').replace(' ', '')
                to_number = f"whatsapp:{phone}"
            else:
                st.warning(f"❌ Keine Telefonnummer für {fehler_row['Name']}")
                return False
        else:
            st.warning("❌ Customer-Daten nicht geladen")
            return False
    else:
        to_number = st.secrets.get("twilio", {}).get("whatsapp_to")
    
    full_name = fehler_row['Name']
    first_name = full_name.split()[0] if ' ' in full_name else full_name
    
    dock = None
    if 'Product_SKU' in fehler_row:
        product = str(fehler_row.get('Product_SKU', ''))
        dock_match = re.search(r'(?:Court|Dock|Platz)\s*(\d+)', product, re.IGNORECASE)
        if dock_match:
            dock = dock_match.group(1)
    
    service_zeit = fehler_row.get('Service_Zeit', '')

    if service_zeit and dock:
        spielinfo = f"Du hast auf Dock {dock} um {service_zeit} Uhr gespielt."
    elif service_zeit:
        spielinfo = f"Du hast um {service_zeit} Uhr gespielt."
    else:
        spielinfo = ""

    message = f"""
🎾 Hey {first_name}!

Schön, dass du bei uns warst – ich hoffe, du hattest eine richtig gute Session auf dem Court! 😊

{spielinfo}

Kleine Bitte:
Wir haben deinen Wellpass-Check-In noch nicht im System. Wär klasse, wenn du ihn schnell nachholen könntest – das hilft uns enorm beim Abgleich.

👉 QR-Code für den Check-In:
{WELLPASS_QR_LINK}

(Tipp: Am besten auf einem zweiten Gerät öffnen oder kurz ausdrucken, dann klappt das Scannen easy.)

Vielen Dank dir und bis ganz bald auf dem Court! 🙌
Liebe Grüße
Michi vom Padel Port

---
_Dies ist eine automatische Nachricht. Bei Rückfragen bitte an info@padel-port.com_
""".strip()

    success = send_whatsapp_message(to_number, message)
    
    if success:
        log_whatsapp_sent(fehler_row, to_number)
    
    return success

def test_whatsapp_connection():
    to_number = st.secrets.get("twilio", {}).get("whatsapp_to")
    
    message = f"""
🎾 *Padel Port Dashboard*

✅ WhatsApp-Integration funktioniert!

QR-Code Link-Test:
{WELLPASS_QR_LINK}

---
_Dies ist eine automatische Nachricht. Bei Rückfragen bitte an info@padel-port.com_
    """.strip()
    
    return send_whatsapp_message(to_number, message)

# ========================================
# CUSTOMER-DATEN
# ========================================

def get_customer_data(player_name):
    customers = loadsheet("customers")
    
    if customers.empty or 'name' not in customers.columns:
        return None
    
    player_name_norm = normalize_name(player_name)
    match = customers[customers['name'].apply(normalize_name) == player_name_norm]
    
    if not match.empty:
        customer = match.iloc[0]
        return {
            'phone_number': customer.get('phone_number', 'Nicht verfügbar'),
            'email': customer.get('email', 'Nicht verfügbar'),
            'category': customer.get('category_name', 'Nicht verfügbar')
        }
    
    return None

# ========================================
# TEST-MODUS
# ========================================

def create_test_fehler_for_michael():
    customers = loadsheet("customers")
    
    if not customers.empty and 'name' in customers.columns:
        michael = customers[customers['name'].str.contains('Michael Osterrieder', case=False, na=False)]
        
        if not michael.empty:
            michael_data = michael.iloc[0]
            
            test_fehler = {
                'Name': michael_data.get('name', 'Michael Osterrieder'),
                'Name_norm': normalize_name(michael_data.get('name', 'Michael Osterrieder')),
                'Betrag': '7.50',
                'Service_Zeit': datetime.now().strftime('%H:%M'),
                'Datum': date.today().strftime('%Y-%m-%d'),
                'Fehler': 'Ja',
                'Relevant': 'Ja',
                'Check-in': 'Nein',
                'Mitarbeiter': 'Nein',
                'Product_SKU': 'Court 2',
                'phone_number': michael_data.get('phone_number', ''),
                'email': michael_data.get('email', ''),
                'category': michael_data.get('category_name', '')
            }
            
            return test_fehler
        else:
            return {
                'Name': 'Michael Osterrieder',
                'Name_norm': normalize_name('Michael Osterrieder'),
                'Betrag': '7.50',
                'Service_Zeit': datetime.now().strftime('%H:%M'),
                'Datum': date.today().strftime('%Y-%m-%d'),
                'Fehler': 'Ja',
                'Relevant': 'Ja',
                'Check-in': 'Nein',
                'Mitarbeiter': 'Nein',
                'Product_SKU': 'Court 2',
                'phone_number': 'Nicht in Customer-Sheet',
                'email': 'Nicht in Customer-Sheet',
                'category': 'Nicht in Customer-Sheet'
            }
    else:
        return None

def render_test_fehler_section():
    st.markdown("---")
    with st.expander("🧪 TEST-MODUS", expanded=False):
        st.markdown("## Test mit echten Customer-Daten")
        
        test_fehler = create_test_fehler_for_michael()
        
        if test_fehler:
            col_info, col_debug, col_action = st.columns([2, 2, 1])
            
            with col_info:
                st.markdown("### 📋 Test-Daten")
                st.caption(f"🧑 {test_fehler['Name']}")
                st.caption(f"⏰ {test_fehler['Service_Zeit']} | 💰 €{test_fehler['Betrag']}")
            
            with col_debug:
                st.markdown("### 🔍 Customer")
                st.caption(f"📱 {test_fehler.get('phone_number', 'N/A')}")
                st.caption(f"📧 {test_fehler.get('email', 'N/A')[:25]}...")
            
            with col_action:
                st.markdown("### 🚀")
                
                if st.button("📱 Test", key="test_wa", type="primary", use_container_width=True):
                    with st.spinner("..."):
                        if send_fehler_notification_with_link(test_fehler, to_player=False):
                            st.success("✅")
                            st.balloons()
                        else:
                            st.error("❌")

# ========================================
# GOOGLE SHEETS
# ========================================

@st.cache_resource
def get_gsheet_client():
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(credentials)
        return client.open_by_key(st.secrets["google_sheets"]["sheet_id"])
    except Exception as e:
        st.error(f"❌ Google Sheets Fehler: {e}")
        return None

@st.cache_data(ttl=300, show_spinner=False)
def loadsheet(name, cols=None):
    try:
        sheet = get_gsheet_client()
        if not sheet:
            return pd.DataFrame(columns=cols) if cols else pd.DataFrame()
        
        try:
            data = sheet.worksheet(name).get_all_records()
            df = pd.DataFrame(data) if data else pd.DataFrame(columns=cols) if cols else pd.DataFrame()
        except gspread.exceptions.WorksheetNotFound:
            sheet.add_worksheet(title=name, rows=1000, cols=20)
            return pd.DataFrame(columns=cols) if cols else pd.DataFrame()
        
        if not df.empty:
            df = optimize_dataframe(df)
        
        return df
    except Exception as e:
        if "429" in str(e):
            st.warning("⚠️ Rate Limit - warte 10s...")
            time.sleep(10)
            loadsheet.clear()
            return loadsheet(name, cols)
        return pd.DataFrame(columns=cols) if cols else pd.DataFrame()

def save_sheet_with_retry(df, name, max_retries=3):
    for attempt in range(max_retries):
        try:
            sheet = get_gsheet_client()
            if not sheet:
                return False
            
            try:
                ws = sheet.worksheet(name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sheet.add_worksheet(title=name, rows=1000, cols=20)
            
            ws.clear()
            time.sleep(0.5)
            
            if not df.empty:
                df_copy = df.copy()
                
                for col in df_copy.columns:
                    if df_copy[col].dtype == 'object' or str(df_copy[col].dtype) == 'category':
                        df_copy[col] = df_copy[col].astype(str).str.replace(',', '.', regex=False)
                    elif df_copy[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                        df_copy[col] = df_copy[col].apply(lambda x: str(x).replace(',', '.') if pd.notna(x) else '')
                
                df_clean = df_copy.fillna('').replace([np.inf, -np.inf], '')
                batch_data = [df_clean.columns.tolist()] + df_clean.values.tolist()
                ws.update(batch_data, value_input_option='RAW')
            
            loadsheet.clear()
            return True
            
        except Exception as e:
            if "429" in str(e) and attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                st.warning(f"⚠️ Rate Limit - {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                st.error(f"❌ Fehler: {e}")
                return False
    
    return False

def savesheet(df, name):
    return save_sheet_with_retry(df, name)

def save_playtomic_raw(df):
    try:
        existing = loadsheet("playtomic_raw")
        
        if not existing.empty and 'Payment id' in existing.columns and 'Payment id' in df.columns:
            def make_key(d):
                payment_id = d.get('Payment id', '')
                club_id = d.get('Club payment id', '')
                key = f"{payment_id}|{club_id}" if payment_id else f"CLUB-{club_id}"
                return key
            
            existing['_key'] = existing.apply(make_key, axis=1)
            df['_key'] = df.apply(make_key, axis=1)
            
            existing_keys = set(existing['_key'].dropna())
            df_new = df[~df['_key'].isin(existing_keys)].copy()
            df_new = df_new.drop('_key', axis=1)
            
            if not df_new.empty:
                existing = existing.drop('_key', axis=1)
                df_combined = pd.concat([existing, df_new], ignore_index=True)
                savesheet(df_combined, "playtomic_raw")
                st.success(f"✅ {len(df_new)} neue Einträge!")
                return True
            else:
                st.info("ℹ️ Keine neuen Daten (alle bereits vorhanden)")
                return False
        else:
            savesheet(df, "playtomic_raw")
            st.success(f"✅ {len(df)} Einträge!")
            return True
            
    except Exception as e:
        st.error(f"❌ Fehler: {e}")
        return False

def get_revenue_from_raw(date_str=None, start_date=None, end_date=None):
    raw_data = loadsheet("playtomic_raw")
    
    if raw_data.empty:
        return {'gesamt': 0, 'reservierung': 0, 'open_match': 0, 'baelle': 0, 'schlaeger': 0, 'sonstige': 0}
    
    raw_data['Service_date_clean'] = raw_data['Service date'].apply(parse_date_safe)
    raw_data = raw_data.dropna(subset=['Service_date_clean'])
    
    if date_str:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        filtered = raw_data[raw_data['Service_date_clean'] == date_obj]
    elif start_date and end_date:
        filtered = raw_data[(raw_data['Service_date_clean'] >= start_date) & (raw_data['Service_date_clean'] <= end_date)]
    else:
        filtered = raw_data
    
    def parse_total(total):
        if pd.isna(total):
            return 0
        total_str = str(total).replace(',', '.').replace('€', '').replace(' ', '').strip()
        try:
            return float(total_str)
        except:
            return 0
    
    filtered['Total_clean'] = filtered['Total'].apply(parse_total)
    gesamt = filtered['Total_clean'].sum()
    
    revenue = {'gesamt': gesamt, 'reservierung': 0, 'open_match': 0, 'baelle': 0, 'schlaeger': 0, 'sonstige': 0}
    
    if 'Product SKU' in filtered.columns:
        for product in filtered['Product SKU'].unique():
            if pd.isna(product):
                continue
            product_revenue = filtered[filtered['Product SKU'] == product]['Total_clean'].sum()
            if 'User booking' in str(product):
                revenue['reservierung'] += product_revenue
            elif 'Open match' in str(product):
                revenue['open_match'] += product_revenue
            elif 'BALLS' in str(product):
                revenue['baelle'] += product_revenue
            elif 'RACKET' in str(product):
                revenue['schlaeger'] += product_revenue
            else:
                revenue['sonstige'] += product_revenue
    
    return revenue

def get_unique_wellpass_checkins(date_str):
    checkins = loadsheet("checkins")
    if checkins.empty or 'analysis_date' not in checkins.columns:
        return 0
    day_checkins = checkins[checkins['analysis_date'] == date_str]
    return day_checkins['Name_norm'].nunique() if not day_checkins.empty else 0

def get_dates():
    buchungen = loadsheet("buchungen", ['analysis_date'])
    if buchungen.empty or 'analysis_date' not in buchungen.columns:
        return []
    dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in buchungen['analysis_date'].unique()]
    return sorted(dates, reverse=True)

def load_snapshot(date_str):
    buchungen = loadsheet("buchungen", ['analysis_date'])
    if buchungen.empty or 'analysis_date' not in buchungen.columns:
        return None
    data = buchungen[buchungen['analysis_date'] == date_str]
    return data if not data.empty else None

def load_checkins_snapshot(date_str):
    checkins = loadsheet("checkins", ['analysis_date'])
    if checkins.empty or 'analysis_date' not in checkins.columns:
        return None
    data = checkins[checkins['analysis_date'] == date_str]
    return data if not data.empty else None

# ========================================
# NAME-MATCHING FUNKTIONEN
# ========================================

def load_name_mapping():
    try:
        df = loadsheet("name_mapping")
        if not df.empty and 'buchung_name' in df.columns and 'checkin_name' in df.columns:
            mapping = {}
            for _, row in df.iterrows():
                mapping[row['buchung_name']] = {
                    'checkin_name': row['checkin_name'],
                    'confidence': row.get('confidence', 100),
                    'timestamp': row.get('timestamp', ''),
                    'confirmed_by': row.get('confirmed_by', 'auto')
                }
            return mapping
        return {}
    except:
        return {}

def save_name_mapping(mapping):
    data = []
    for buchung_name, details in mapping.items():
        if isinstance(details, dict):
            data.append({
                'buchung_name': buchung_name,
                'checkin_name': details['checkin_name'],
                'confidence': details.get('confidence', 100),
                'timestamp': details.get('timestamp', datetime.now().isoformat()),
                'confirmed_by': details.get('confirmed_by', 'auto')
            })
        else:
            data.append({
                'buchung_name': buchung_name,
                'checkin_name': details,
                'confidence': 100,
                'timestamp': datetime.now().isoformat(),
                'confirmed_by': 'legacy'
            })
    
    df = pd.DataFrame(data)
    savesheet(df, "name_mapping")

def load_rejected_matches():
    try:
        df = loadsheet("rejected_matches")
        if not df.empty:
            return set(tuple(row) for row in df[['buchung_name', 'checkin_name']].values)
        return set()
    except:
        return set()

def save_rejected_match(buchung_name, checkin_name):
    df = loadsheet("rejected_matches", cols=['buchung_name', 'checkin_name', 'timestamp'])
    new_row = pd.DataFrame([{
        'buchung_name': buchung_name,
        'checkin_name': checkin_name,
        'timestamp': datetime.now().isoformat()
    }])
    df = pd.concat([df, new_row], ignore_index=True)
    savesheet(df, "rejected_matches")

def remove_rejected_match(buchung_name, checkin_name):
    df = loadsheet("rejected_matches", cols=['buchung_name', 'checkin_name', 'timestamp'])
    if not df.empty:
        df = df[~((df['buchung_name'] == buchung_name) & (df['checkin_name'] == checkin_name))]
        savesheet(df, "rejected_matches")

def check_initials_match(name1, name2):
    def get_initials(name):
        parts = name.split()
        return ''.join([p[0].lower() for p in parts if p])
    
    init1 = get_initials(name1)
    init2 = get_initials(name2)
    return init1 in init2 or init2 in init1 or init1 == init2

def phonetic_similarity(name1, name2):
    def simplify_phonetic(name):
        if not name:
            return ""
        simplified = name[0].lower()
        for char in name[1:].lower():
            if char not in 'aeiouäöü':
                simplified += char
        replacements = {
            'z': 's', 'c': 'k', 'v': 'f', 'w': 'v',
            'ph': 'f', 'dt': 't', 'th': 't'
        }
        for old, new in replacements.items():
            simplified = simplified.replace(old, new)
        return simplified
    
    s1 = simplify_phonetic(name1)
    s2 = simplify_phonetic(name2)
    return fuzz.ratio(s1, s2)

def advanced_fuzzy_match(query_name, candidate_names, mapping, rejected_matches, already_matched_checkins=None):
    if not candidate_names:
        return []
    
    if already_matched_checkins is None:
        already_matched_checkins = set()
    
    available_candidates = [c for c in candidate_names if c not in already_matched_checkins]
    
    if not available_candidates:
        return []
    
    if query_name in mapping:
        learned = mapping[query_name]
        learned_name = learned['checkin_name'] if isinstance(learned, dict) else learned
        if learned_name in available_candidates:
            return [(learned_name, 100, 'learned')]
    
    results = []
    
    for candidate in available_candidates:
        if (query_name, candidate) in rejected_matches:
            continue
        
        token_score = fuzz.token_set_ratio(query_name, candidate)
        partial_score = fuzz.partial_ratio(query_name, candidate)
        initials_match = check_initials_match(query_name, candidate)
        initials_bonus = 20 if initials_match else 0
        phonetic_score = phonetic_similarity(query_name, candidate)
        
        final_score = (
            token_score * 0.5 + 
            partial_score * 0.2 + 
            phonetic_score * 0.2 + 
            initials_bonus
        )
        
        if final_score > 50:
            results.append((candidate, round(final_score, 1), 'fuzzy'))
    
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:5]

def render_name_matching_interface(fehler_row, ci_df, mapping, rejected_matches, all_fehler):
    name = fehler_row['Name_norm']
    key_base = f"{fehler_row['Name_norm']}_{fehler_row['Datum']}_{fehler_row['Betrag']}"
    
    checkin_names = list(ci_df['Name_norm']) if ci_df is not None and not ci_df.empty else []
    
    already_matched = set()
    for _, other_fehler in all_fehler.iterrows():
        if other_fehler['Name_norm'] == name:
            continue
        other_name = other_fehler['Name_norm']
        if other_name in mapping:
            mapped = mapping[other_name]
            mapped_name = mapped['checkin_name'] if isinstance(mapped, dict) else mapped
            already_matched.add(mapped_name)
    
    matches = advanced_fuzzy_match(name, checkin_names, mapping, rejected_matches, already_matched)
    
    if not matches:
        st.info("💡 Keine Vorschläge (Score > 50% oder bereits zugeordnet)")
        
        st.markdown("### ✏️ Manuell")
        col1, col2 = st.columns([3, 1])
        with col1:
            manual_match = st.selectbox(
                "Name wählen:",
                options=[''] + [ci_df[ci_df['Name_norm'] == n].iloc[0]['Name'] for n in checkin_names],
                key=f"manual_only_{key_base}",
                label_visibility="collapsed"
            )
        with col2:
            if st.button("💾", key=f"save_manual_only_{key_base}", disabled=not manual_match, use_container_width=True):
                if manual_match:
                    manual_norm = ci_df[ci_df['Name'] == manual_match].iloc[0]['Name_norm']
                    mapping[name] = {
                        'checkin_name': manual_norm,
                        'confidence': 100,
                        'timestamp': datetime.now().isoformat(),
                        'confirmed_by': 'manual'
                    }
                    save_name_mapping(mapping)
                    
                    corr = loadsheet("corrections", ['key','date','behoben','timestamp'])
                    key = f"{fehler_row['Name_norm']}_{fehler_row['Datum']}_{fehler_row['Betrag']}"
                    if not corr.empty and 'key' in corr.columns:
                        corr = corr[corr['key'] != key]
                    new_row_df = pd.DataFrame([{
                        'key': key,
                        'date': fehler_row['Datum'],
                        'behoben': True,
                        'timestamp': datetime.now().isoformat()
                    }])
                    corr = pd.concat([corr, new_row_df], ignore_index=True)
                    savesheet(corr, "corrections")
                    
                    st.success("✅ Match gespeichert & behoben!")
                    time.sleep(0.5)
                    st.rerun()
        return
    
    st.markdown("### 🔍 Vorschläge")
    
    for i, (match_name, score, match_type) in enumerate(matches):
        original_match = ci_df[ci_df['Name_norm'] == match_name].iloc[0]['Name']
        
        if match_type == 'learned':
            confidence = "✅ GELERNT"
        elif score >= 90:
            confidence = "🟢 SICHER"
        elif score >= 75:
            confidence = "🟡 WAHRSCH."
        else:
            confidence = "🔴 UNSICHER"
        
        with st.expander(f"**{i+1}.** {original_match} - {score}% - {confidence}", 
                        expanded=(i == 0 and score >= 75)):
            
            col_info, col_actions = st.columns([3, 1])
            
            with col_info:
                st.caption(f"📝 {fehler_row['Name']} → ✅ {original_match} | 📊 {score}%")
                
                if check_initials_match(name, match_name):
                    st.caption("💡 Initialen ✓")
            
            with col_actions:
                if st.button("✅", key=f"confirm_{key_base}_{i}", 
                           type="primary" if score >= 85 else "secondary",
                           use_container_width=True):
                    
                    mapping[name] = {
                        'checkin_name': match_name,
                        'confidence': score,
                        'timestamp': datetime.now().isoformat(),
                        'confirmed_by': 'user'
                    }
                    save_name_mapping(mapping)
                    
                    if (name, match_name) in rejected_matches:
                        remove_rejected_match(name, match_name)
                    
                    corr = loadsheet("corrections", ['key','date','behoben','timestamp'])
                    key = f"{fehler_row['Name_norm']}_{fehler_row['Datum']}_{fehler_row['Betrag']}"
                    if not corr.empty and 'key' in corr.columns:
                        corr = corr[corr['key'] != key]
                    new_row_df = pd.DataFrame([{
                        'key': key,
                        'date': fehler_row['Datum'],
                        'behoben': True,
                        'timestamp': datetime.now().isoformat()
                    }])
                    corr = pd.concat([corr, new_row_df], ignore_index=True)
                    savesheet(corr, "corrections")
                    
                    st.success("✅")
                    time.sleep(0.5)
                    st.rerun()
                
                if st.button("❌", key=f"reject_{key_base}_{i}", use_container_width=True):
                    save_rejected_match(name, match_name)
                    st.warning("❌")
                    time.sleep(0.5)
                    st.rerun()
    
    st.markdown("---")
    st.markdown("### ✏️ Manuell")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        manual_match = st.selectbox(
            "Name wählen:",
            options=[''] + [ci_df[ci_df['Name_norm'] == n].iloc[0]['Name'] for n in checkin_names],
            key=f"manual_{key_base}",
            label_visibility="collapsed"
        )
    with col2:
        if st.button("💾", key=f"save_manual_{key_base}", disabled=not manual_match, use_container_width=True):
            if manual_match:
                manual_norm = ci_df[ci_df['Name'] == manual_match].iloc[0]['Name_norm']
                mapping[name] = {
                    'checkin_name': manual_norm,
                    'confidence': 100,
                    'timestamp': datetime.now().isoformat(),
                    'confirmed_by': 'manual'
                }
                save_name_mapping(mapping)
                
                corr = loadsheet("corrections", ['key','date','behoben','timestamp'])
                key = f"{fehler_row['Name_norm']}_{fehler_row['Datum']}_{fehler_row['Betrag']}"
                if not corr.empty and 'key' in corr.columns:
                    corr = corr[corr['key'] != key]
                new_row_df = pd.DataFrame([{
                    'key': key,
                    'date': fehler_row['Datum'],
                    'behoben': True,
                    'timestamp': datetime.now().isoformat()
                }])
                corr = pd.concat([corr, new_row_df], ignore_index=True)
                savesheet(corr, "corrections")
                
                st.success("✅")
                time.sleep(0.5)
                st.rerun()

def render_learned_matches_manager():
    st.markdown("---")
    
    mapping = load_name_mapping()
    rejected = load_rejected_matches()
    
    with st.expander(f"🧠 Gelernte Matches ({len(mapping)} ✅, {len(rejected)} ❌)", expanded=False):
        
        if not mapping and not rejected:
            st.info("Noch keine gelernten Matches")
            return
        
        tab_learned, tab_rejected = st.tabs(["✅ Bestätigt", "❌ Abgelehnt"])
        
        with tab_learned:
            if mapping:
                st.caption(f"**{len(mapping)} Matches**")
                
                for buchung_name, details in mapping.items():
                    if isinstance(details, dict):
                        checkin_name = details['checkin_name']
                        confidence = details.get('confidence', 100)
                        confirmed_by = details.get('confirmed_by', 'auto')
                    else:
                        checkin_name = details
                        confidence = 100
                        confirmed_by = 'legacy'
                    
                    col1, col2, col3 = st.columns([3, 2, 1])
                    
                    with col1:
                        st.caption(f"{buchung_name} → {checkin_name}")
                    with col2:
                        st.caption(f"{confidence}% ({confirmed_by})")
                    with col3:
                        if st.button("🗑️", key=f"del_map_{buchung_name}"):
                            del mapping[buchung_name]
                            save_name_mapping(mapping)
                            st.success("Gelöscht!")
                            time.sleep(0.5)
                            st.rerun()
            else:
                st.info("Keine bestätigten Matches")
        
        with tab_rejected:
            if rejected:
                st.caption(f"**{len(rejected)} Matches**")
                
                rejected_list = list(rejected)
                for buchung_name, checkin_name in rejected_list:
                    col1, col2 = st.columns([4, 1])
                    
                    with col1:
                        st.caption(f"{buchung_name} ≠ {checkin_name}")
                    with col2:
                        if st.button("↩️", key=f"restore_{buchung_name}_{checkin_name}"):
                            remove_rejected_match(buchung_name, checkin_name)
                            st.success("Wiederhergestellt!")
                            time.sleep(0.5)
                            st.rerun()
            else:
                st.info("Keine abgelehnten Matches")

# ========================================
# MAIN APP
# ========================================

st.set_page_config(page_title="Padel Port Dashboard", layout="wide", page_icon="🎾")

st.markdown("""
<style>
    .stExpander {
        margin-bottom: 0.5rem !important;
    }
    .stExpander > div {
        padding: 0.5rem !important;
    }
    .element-container {
        margin-bottom: 0.3rem !important;
    }
    .stExpander p, .stExpander span {
        font-size: 0.9rem !important;
    }
    .stExpander h3 {
        font-size: 1rem !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.3rem !important;
    }
</style>
""", unsafe_allow_html=True)

validate_secrets()

if not check_password():
    st.stop()

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'current_date' not in st.session_state:
    st.session_state.current_date = date.today().strftime("%Y-%m-%d")
if 'df_all' not in st.session_state:
    st.session_state.df_all = pd.DataFrame()
if 'checkins_all' not in st.session_state:
    st.session_state.checkins_all = pd.DataFrame()
if 'day_idx' not in st.session_state:
    st.session_state.day_idx = 0

if not st.session_state.data_loaded:
    dates = get_dates()
    if dates:
        latest_date = dates[0]
        snap = load_snapshot(latest_date.strftime("%Y-%m-%d"))
        ci_snap = load_checkins_snapshot(latest_date.strftime("%Y-%m-%d"))
        
        if snap is not None:
            st.session_state.df_all = snap
            st.session_state.checkins_all = ci_snap if ci_snap is not None else pd.DataFrame()
            st.session_state.current_date = latest_date.strftime("%Y-%m-%d")
            st.session_state.data_loaded = True

st.markdown("<h1 style='text-align: center;'>🎾 Padel Port Dashboard</h1>", unsafe_allow_html=True)

# ========================================
# SIDEBAR
# ========================================

st.sidebar.title("🚀 Neue Analyse")

p_file = st.sidebar.file_uploader("📁 Playtomic CSV", type=['csv'], key="playtomic")
c_file = st.sidebar.file_uploader("📁 Checkins CSV", type=['csv'], key="checkins")

# ✅ PLAYTOMIC/CHECKIN ANALYSE
if st.sidebar.button("🚀 Analysieren", use_container_width=True) and p_file and c_file:
    with st.spinner("🔄 Verarbeite..."):
        pdf = pd.read_csv(p_file, sep=';', skiprows=3, engine='python', on_bad_lines='skip', encoding='utf-8')
        save_playtomic_raw(pdf)
        
        playtomic_filtered = pdf[pdf['Product SKU'].isin(['User booking registration', 'Open match registration'])].copy() if 'Product SKU' in pdf.columns else pdf.copy()
        
        if 'Refund id' in playtomic_filtered.columns:
            playtomic_filtered = playtomic_filtered[playtomic_filtered['Refund id'] == '-']
        if 'Payment status' in playtomic_filtered.columns:
            playtomic_filtered = playtomic_filtered[playtomic_filtered['Payment status'] != 'Refund']
        
        rename_map = {
            'User name':'Name', 
            'Total':'Betrag_raw', 
            'Service date':'Servicedatum_raw', 
            'Product SKU': 'Product_SKU',
            'Payment id': 'Payment id',
            "Club payment id": "Club payment id"
        }
        if 'Service time' in playtomic_filtered.columns:
            rename_map['Service time'] = 'Service_Zeit'
        
        playtomic_filtered.rename(columns=rename_map, inplace=True)

        playtomic_filtered['Service_Zeit'] = playtomic_filtered['Servicedatum_raw'].astype(str).str.extract(r'(\d{2}:\d{2})')

        playtomic_filtered['Name_norm'] = playtomic_filtered['Name'].apply(normalize_name)
        playtomic_filtered['Betrag_raw'] = (
            playtomic_filtered['Betrag_raw']
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .str.replace('€', '', regex=False)
            .str.strip()
        )
        playtomic_filtered['Betrag'] = pd.to_numeric(playtomic_filtered['Betrag_raw'], errors='coerce').fillna(0)

        playtomic_filtered['Betrag'] = playtomic_filtered['Betrag'].apply(lambda x: f"{x:.2f}".replace(',', '.'))
        playtomic_filtered['Servicedatum'] = playtomic_filtered['Servicedatum_raw'].apply(parse_date_safe)
        
        if 'Service_Zeit' not in playtomic_filtered.columns:
            playtomic_filtered['Service_Zeit'] = ''
        else:
            playtomic_filtered['Service_Zeit'] = playtomic_filtered['Service_Zeit'].fillna('')
        
        playtomic_filtered['Betrag_num'] = pd.to_numeric(playtomic_filtered['Betrag'], errors='coerce').fillna(0)
        playtomic_filtered = playtomic_filtered[playtomic_filtered['Betrag_num'] >= 0]
        if 'Payment id' in playtomic_filtered.columns:
            playtomic_filtered = playtomic_filtered.drop_duplicates(subset=['Payment id'])

        
        playtomic_filtered['Relevant'] = (
            ((playtomic_filtered['Betrag_num'] < 7) & (playtomic_filtered['Betrag_num'] > 0)) | 
            (playtomic_filtered['Betrag_num'] == 10) | 
            (playtomic_filtered['Betrag_num'] == 0)
        )
        
        cdf = parse_csv(c_file)
        rename_map_ci = {'Vor- & Nachname':'Name', 'Datum':'Checkin_Datum_raw'}
        if 'Zeit' in cdf.columns:
            rename_map_ci['Zeit'] = 'Checkin_Zeit'
        cdf.rename(columns=rename_map_ci, inplace=True)
        cdf['Name_norm'] = cdf['Name'].apply(normalize_name)
        cdf['Checkin_Datum'] = pd.to_datetime(cdf['Checkin_Datum_raw'], errors='coerce').dt.date
        
        if 'Checkin_Zeit' not in cdf.columns:
            cdf['Checkin_Zeit'] = ''
        else:
            cdf['Checkin_Zeit'] = cdf['Checkin_Zeit'].fillna('')
        
        all_dates = sorted(set(playtomic_filtered['Servicedatum'].dropna()) | set(cdf['Checkin_Datum'].dropna()))
        st.info(f"📦 Verarbeite {len(all_dates)} Tage...")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_results = []
        all_checkin_results = []
        
        mapping = load_name_mapping()
        
        for i, td in enumerate(all_dates):
            progress = (i + 1) / len(all_dates)
            progress_bar.progress(progress)
            status_text.text(f"{td.strftime('%d.%m.%Y')} ({i+1}/{len(all_dates)})")
            
            pd_day = playtomic_filtered[playtomic_filtered['Servicedatum'] == td]
            cd_day = cdf[cdf['Checkin_Datum'] == td]
            
            results = []
            for _, row in pd_day.iterrows():
                is_ma = normalize_name(row['Name']) in [normalize_name(m) for m in MITARBEITER]
                
                checkin_match = cd_day[cd_day['Name_norm'] == row['Name_norm']]
                has_ci = not checkin_match.empty
                
                if not has_ci and row['Name_norm'] in mapping:
                    mapped = mapping[row['Name_norm']]
                    mapped_name = mapped['checkin_name'] if isinstance(mapped, dict) else mapped
                    
                    mapped_checkin = cd_day[cd_day['Name_norm'] == mapped_name]
                    if not mapped_checkin.empty:
                        has_ci = True
                        checkin_match = mapped_checkin
                
                ci_zeit = checkin_match.iloc[0]['Checkin_Zeit'] if has_ci else ''
                fehler = row['Relevant'] and not has_ci and not is_ma
                
                results.append({
                    'Datum': str(td),
                    'Name': row['Name'],
                    'Name_norm': row['Name_norm'],
                    'Betrag': row['Betrag'],
                    'Service_Zeit': str(row['Service_Zeit']),
                    'Checkin_Zeit': str(ci_zeit),
                    'Product_SKU': row.get('Product_SKU', ''),
                    'Relevant': 'Ja' if row['Relevant'] else 'Nein',
                    'Check-in': 'Ja' if has_ci else 'Nein',
                    'Mitarbeiter': 'Ja' if is_ma else 'Nein',
                    'Fehler': 'Ja' if fehler else 'Nein',
                    'analysis_date': td.strftime("%Y-%m-%d"),
                    'Payment id': row.get('Payment id', ''),
                    'Club payment id': row.get('Club payment id', '')
                })
            
            all_results.extend(results)
            
            checkin_results = []
            seen_names = set()
            for _, row in cd_day.iterrows():
                if row['Name_norm'] not in seen_names:
                    seen_names.add(row['Name_norm'])
                    buchung = pd_day[pd_day['Name_norm'] == row['Name_norm']]
                    gespielt = not buchung.empty
                    checkin_results.append({
                        'Datum': str(td), 'Name': row['Name'], 'Name_norm': row['Name_norm'],
                        'Checkin_Zeit': str(row['Checkin_Zeit']), 
                        'Gespielt': 'Ja' if gespielt else 'Nein',
                        'analysis_date': td.strftime("%Y-%m-%d")
                    })
            
            all_checkin_results.extend(checkin_results)
        
        progress_bar.progress(1.0)
        status_text.success(f"✅ {len(all_dates)} Tage verarbeitet!")
        
        st.info("💾 Speichere...")
        
        # ✅ DUPLIKAT-FILTERUNG BUCHUNGEN
        if all_results:
            buchungen = loadsheet("buchungen", ['analysis_date'])
            
            if not buchungen.empty:
                buchungen['_dup_key'] = (
                    buchungen['analysis_date'].astype(str) + '|' + 
                    buchungen['Name_norm'].astype(str) + '|' + 
                    buchungen['Service_Zeit'].astype(str)
                )
                existing_keys = set(buchungen['_dup_key'])
                
                new_results_df = pd.DataFrame(all_results)
                new_results_df['_dup_key'] = (
                    new_results_df['analysis_date'].astype(str) + '|' + 
                    new_results_df['Name_norm'].astype(str) + '|' + 
                    new_results_df['Service_Zeit'].astype(str)
                )
                
                new_results_filtered = new_results_df[~new_results_df['_dup_key'].isin(existing_keys)]
                new_results_filtered = new_results_filtered.drop('_dup_key', axis=1)
                
                duplicates_found = len(new_results_df) - len(new_results_filtered)
                
                if duplicates_found > 0:
                    st.warning(f"⚠️ {duplicates_found} Duplikate übersprungen")
                
                if not new_results_filtered.empty:
                    buchungen = buchungen.drop('_dup_key', axis=1)
                    new_buchungen = pd.concat([buchungen, new_results_filtered], ignore_index=True)
                    savesheet(new_buchungen, "buchungen")
                    st.success(f"✅ {len(new_results_filtered)} neue Buchungen!")
                else:
                    st.info("ℹ️ Keine neuen Buchungen (alle vorhanden)")
            else:
                new_buchungen = pd.DataFrame(all_results)
                savesheet(new_buchungen, "buchungen")
                st.success(f"✅ {len(all_results)} Buchungen!")
        
        # ✅ DUPLIKAT-FILTERUNG CHECK-INS
        if all_checkin_results:
            checkins = loadsheet("checkins", ['analysis_date'])
            
            if not checkins.empty:
                checkins['_dup_key'] = (
                    checkins['analysis_date'].astype(str) + '|' + 
                    checkins['Name_norm'].astype(str) + '|' + 
                    checkins['Checkin_Zeit'].astype(str)
                )
                existing_keys = set(checkins['_dup_key'])
                
                new_checkins_df = pd.DataFrame(all_checkin_results)
                new_checkins_df['_dup_key'] = (
                    new_checkins_df['analysis_date'].astype(str) + '|' + 
                    new_checkins_df['Name_norm'].astype(str) + '|' + 
                    new_checkins_df['Checkin_Zeit'].astype(str)
                )
                
                new_checkins_filtered = new_checkins_df[~new_checkins_df['_dup_key'].isin(existing_keys)]
                new_checkins_filtered = new_checkins_filtered.drop('_dup_key', axis=1)
                
                duplicates_found = len(new_checkins_df) - len(new_checkins_filtered)
                
                if duplicates_found > 0:
                    st.warning(f"⚠️ {duplicates_found} Check-in-Duplikate übersprungen")
                
                if not new_checkins_filtered.empty:
                    checkins = checkins.drop('_dup_key', axis=1)
                    new_checkins = pd.concat([checkins, new_checkins_filtered], ignore_index=True)
                    savesheet(new_checkins, "checkins")
                    st.success(f"✅ {len(new_checkins_filtered)} neue Check-ins!")
                else:
                    st.info("ℹ️ Keine neuen Check-ins (alle vorhanden)")
            else:
                new_checkins = pd.DataFrame(all_checkin_results)
                savesheet(new_checkins, "checkins")
                st.success(f"✅ {len(all_checkin_results)} Check-ins!")
        
        st.success(f"🎉 Abgeschlossen!")
        st.balloons()
        
        time.sleep(2)
        st.rerun()

        
# ========================================
# ✅ CUSTOMER UPLOAD
# ========================================

st.sidebar.markdown("---")
st.sidebar.title("👥 Customer")

cust_file = st.sidebar.file_uploader("📁 Customer CSV", type=['csv'], key="customers")

if st.sidebar.button("📤 Hochladen", use_container_width=True, type="primary") and cust_file:
    with st.spinner("🔄 Lade..."):
        try:
            customers_df = parse_csv(cust_file)
            
            if customers_df.empty:
                st.sidebar.error("❌ CSV leer")
            elif 'name' not in customers_df.columns:
                st.sidebar.error(f"❌ 'name' fehlt! Gefunden: {', '.join(customers_df.columns[:5])}")
            else:
                customers_df['name_norm'] = customers_df['name'].apply(normalize_name)
                
                if savesheet(customers_df, "customers"):
                    st.sidebar.success(f"✅ {len(customers_df)} Kunden!")
                    loadsheet.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.sidebar.error("❌ Speichern fehlgeschlagen")
        except Exception as e:
            st.sidebar.error(f"❌ {str(e)[:50]}")

# ========================================
# TABS
# ========================================

tab1, tab2 = st.tabs(["📅 Tag", "📊 Monat"])

with tab1:
    dates = get_dates()
    
    if not dates:
        st.info("🔄 Lade CSVs hoch!")
        st.stop()
    
    # Navigation
    col_prev, col_date, col_next = st.columns([1, 3, 1])
    
    with col_prev:
        if st.button("◀ Vorheriger", use_container_width=True, key="prev_btn"):
            new_idx = min(st.session_state.day_idx + 1, len(dates) - 1)
            st.session_state.day_idx = new_idx
            st.session_state.current_date = dates[new_idx].strftime("%Y-%m-%d")
            st.rerun()
    
    with col_date:
        curr_date = dates[st.session_state.day_idx]
        st.info(f"📅 {curr_date.strftime('%d.%m.%Y')} (Tag {st.session_state.day_idx + 1}/{len(dates)})")
    
    with col_next:
        if st.button("Nächster ▶", use_container_width=True, key="next_btn"):
            new_idx = max(st.session_state.day_idx - 1, 0)
            st.session_state.day_idx = new_idx
            st.session_state.current_date = dates[new_idx].strftime("%Y-%m-%d")
            st.rerun()
    
    with st.expander("📆 Datum direkt wählen", expanded=False):
        selected_date = st.selectbox(
            "Wähle:",
            options=dates,
            index=st.session_state.day_idx,
            format_func=lambda x: x.strftime("%d.%m.%Y"),
            key="date_jump"
        )
        
        if st.button("✅ Springen", use_container_width=True):
            st.session_state.day_idx = dates.index(selected_date)
            st.session_state.current_date = selected_date.strftime("%Y-%m-%d")
            st.rerun()
    
    curr_date = dates[st.session_state.day_idx]
    st.session_state.current_date = curr_date.strftime("%Y-%m-%d")
    
    date_str = curr_date.strftime("%A, %d. %B %Y")
    for en, de in {
        "Monday": "Montag", "Tuesday": "Dienstag", "Wednesday": "Mittwoch",
        "Thursday": "Donnerstag", "Friday": "Freitag", "Saturday": "Samstag", "Sunday": "Sonntag",
        "January": "Januar", "February": "Februar", "March": "März", "April": "April",
        "May": "Mai", "June": "Juni", "July": "Juli", "August": "August",
        "September": "September", "October": "Oktober", "November": "November", "December": "Dezember"
    }.items():
        date_str = date_str.replace(en, de)
    
    st.markdown(f"<h2 style='text-align: center; color: #2c3e50;'>📅 {date_str}</h2>", unsafe_allow_html=True)
    st.markdown("---")
    
    df = load_snapshot(st.session_state.current_date)
    ci_df = load_checkins_snapshot(st.session_state.current_date)
    
    if df is None or df.empty:
        st.info("Keine Daten für diesen Tag.")
        st.stop()

    revenue = get_revenue_from_raw(date_str=st.session_state.current_date)
    wellpass_unique_checkins = get_unique_wellpass_checkins(st.session_state.current_date)
    
    current_day = datetime.strptime(st.session_state.current_date, "%Y-%m-%d").date()
    wellpass_wert_tag = get_wellpass_wert(current_day)
        
    wellpass_revenue = wellpass_unique_checkins * wellpass_wert_tag
    gesamt_mit_wellpass = revenue['gesamt'] + wellpass_revenue
    
    
    st.subheader("💰 Umsatz")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("💰 Gesamt", f"€{gesamt_mit_wellpass:.2f}")
    with col2:
        pct = (revenue['gesamt'] / gesamt_mit_wellpass * 100) if gesamt_mit_wellpass > 0 else 0
        st.metric("🎾 Playtomic", f"€{revenue['gesamt']:.2f}", f"{pct:.0f}%")
    with col3:
        pct = (revenue['reservierung'] / gesamt_mit_wellpass * 100) if gesamt_mit_wellpass > 0 else 0
        st.metric("🏟️ Courts", f"€{revenue['reservierung']:.2f}", f"{pct:.0f}%")
    with col4:
        pct = (revenue['open_match'] / gesamt_mit_wellpass * 100) if gesamt_mit_wellpass > 0 else 0
        st.metric("🏆 Matches", f"€{revenue['open_match']:.2f}", f"{pct:.0f}%")
    with col5:
        extras = revenue['baelle'] + revenue['schlaeger']
        pct = (extras / gesamt_mit_wellpass * 100) if gesamt_mit_wellpass > 0 else 0
        st.metric("🎾 Extras", f"€{extras:.2f}", f"{pct:.0f}%")
    with col6:
        pct = (wellpass_revenue / gesamt_mit_wellpass * 100) if gesamt_mit_wellpass > 0 else 0
        st.metric("💳 Wellpass", f"€{wellpass_revenue:.2f}", f"{wellpass_unique_checkins} P. ({pct:.0f}%)")
    
    if gesamt_mit_wellpass > 0:
        fig = go.Figure(data=[go.Pie(
            labels=['Courts', 'Matches', 'Extras', 'Wellpass'],
            values=[revenue['reservierung'], revenue['open_match'], extras, wellpass_revenue],
            hole=.3
        )])
        fig.update_layout(title="Verteilung", height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("🎾 Buchungen", len(df))
    with col2:
        st.metric("🎯 Relevant", len(df[df['Relevant'] == 'Ja']))
    with col3:
        st.metric("❌ Fehler", len(df[df['Fehler'] == 'Ja']))
    with col4:
        st.metric("👥 MA", len(df[df['Mitarbeiter'] == 'Ja']))
    with col5:
        st.metric("✅ Check-ins", len(ci_df) if ci_df is not None else 0)
    
    st.markdown("---")
    
    ct1, ct2 = st.columns(2)
    
    # Linke Tabelle: relevante Buchungen
    with ct1:
        rv = df[df['Relevant'] == 'Ja'].sort_values('Name').copy()
        display_cols = ['Name', 'Betrag']
        if 'Service_Zeit' in rv.columns:
            display_cols.append('Service_Zeit')
        display_cols.extend(['Check-in', 'Fehler'])
        styled = rv[display_cols].style.map(color_fehler, subset=['Fehler'])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=min(len(rv) * 35 + 38, 800))
        st.caption(f"📊 {len(rv)} Relevant")
    
    # Rechte Tabelle: Check-ins mit "Gespielt" (inkl. Name-Mapping)
    with ct2:
        
        if ci_df is not None and not ci_df.empty:
            # Kopie + sichergehen, dass die Norm-Spalte existiert
            ci_view = ci_df.sort_values('Name').copy()
            if 'Name_norm' not in ci_view.columns:
                ci_view['Name_norm'] = ci_view['Name'].apply(normalize_name)
    
        # aktuelles Dashboard-Datum als Fallback für Checkins
            current_date = datetime.strptime(st.session_state.current_date, "%Y-%m-%d").date()

        # Name-Mapping laden (buchung_name_norm -> checkin_name_norm)
            mapping = load_name_mapping()
    
        # Invertiertes Mapping: checkin_name_norm -> {buchung_name_norms}
            inverse_mapping = {}
            for buchung_name_norm, details in mapping.items():
                if isinstance(details, dict):
                    ci_norm = details.get("checkin_name", "")
                else:
                    ci_norm = details
                if not ci_norm:
                    continue
                inverse_mapping.setdefault(ci_norm, set()).add(buchung_name_norm)

            gespielt_list = []

            for _, ci_row in ci_view.iterrows():
                ci_name = ci_row.get("Name", "")
                ci_name_norm = ci_row.get("Name_norm", "")
            # In vielen Fällen gibt es keine eigene Datumsspalte im Checkin-Snapshot → Dashboard-Datum nutzen
                ci_datum = ci_row.get("Checkin_Datum", current_date)
                if pd.isna(ci_datum):
                    ci_datum = current_date

                if not ci_name_norm:
                    gespielt_list.append("Nein")
                    continue

                # Alle Buchungen dieses Tages, die NICHT Mitarbeiter sind
                day_bookings = df[
                    (df["Datum"].astype(str) == str(ci_datum)) &
                    (df["Mitarbeiter"] != "Ja")
                ]

                if day_bookings.empty:
                    gespielt_list.append("Nein")
                    continue

                found = False

                # 1) Direkter Match über Name_norm
                if "Name_norm" in day_bookings.columns:
                    if ci_name_norm in list(day_bookings["Name_norm"]):
                        found = True

            # 2) Wenn nicht gefunden: Mapping nutzen
                if not found and ci_name_norm in inverse_mapping:
                    for buchung_name_norm in inverse_mapping[ci_name_norm]:
                        b_match = day_bookings[day_bookings["Name_norm"] == buchung_name_norm]
                        if not b_match.empty:
                            found = True
                            break

            # 3) Optionaler, einfacher Fallback-Vergleich (Nachname + ähnlicher Vorname)
                if not found:
                    ci_name_lower = ci_name.lower().strip()
                    ci_parts = ci_name_lower.split()

                    for _, booking in day_bookings.iterrows():
                        booking_name = str(booking.get("Name", "")).lower().strip()
                        b_parts = booking_name.split()

                        if booking_name == ci_name_lower:
                            found = True
                            break

                        if len(ci_parts) >= 2 and len(b_parts) >= 2:
                            if ci_parts[-1] == b_parts[-1]:
                                if ci_parts[0] in b_parts[0] or b_parts[0] in ci_parts[0]:
                                    found = True
                                    break

                gespielt_list.append("Ja" if found else "Nein")

            ci_view["Gespielt"] = gespielt_list

            display_cols = ["Name", "Checkin_Zeit", "Gespielt"]

            def color_gespielt(val: str) -> str:
                if val == "Ja":
                    return "background-color: #d4edda; color: #155724"
                if val == "Nein":
                    return "background-color: #f8d7da; color: #721c24"
                return ""

            styled_ci = ci_view[display_cols].style.map(color_gespielt, subset=["Gespielt"])
            st.dataframe(
                styled_ci,
                use_container_width=True,
                hide_index=True,
                height=min(len(ci_view) * 35 + 38, 800),
            )
            st.caption(f"📊 {len(ci_view)} Check-ins (Unique: {wellpass_unique_checkins})")
        else:
            st.info("Keine Check-ins")




    st.markdown("---")
    
    fehler = df[df['Fehler'] == 'Ja'].copy()
    if not fehler.empty:
        st.subheader(f"🛑 Offene Wellpass-Fehler ({len(fehler)})")
        
        st.info(f"💬 {len(fehler)} Fehler gefunden")
        
        st.markdown("---")
        
        mapping = load_name_mapping()
        rejected_matches = load_rejected_matches()
        corr = loadsheet("corrections", ['key','date','behoben','timestamp'])
        
        fehler_data = []
        
        for idx, row in fehler.iterrows():
            key = f"{row['Name_norm']}_{row['Datum']}_{row['Betrag']}"
            
            is_behoben = False
            if not corr.empty and 'key' in corr.columns:
                match = corr[corr['key'] == key]
                if not match.empty:
                    is_behoben = bool(match.iloc[0].get('behoben', False))
            
            whatsapp_sent_time = get_whatsapp_sent_time(row)
            customer_data = get_customer_data(row['Name'])
            
            telefon = 'N/A'
            if customer_data and customer_data['phone_number'] != 'Nicht verfügbar':
                tel = customer_data['phone_number']
                telefon = tel[:15] + '...' if len(tel) > 15 else tel
            
            fehler_data.append({
                'Status': '✅' if is_behoben else '🔴',
                'Name': row['Name'],
                'Betrag': f"€{row['Betrag']}",
                'Zeit': row.get('Service_Zeit', 'N/A'),
                'Telefon': telefon,
                'WhatsApp': '✅ ' + whatsapp_sent_time.strftime("%d.%m. %H:%M") if whatsapp_sent_time else '❌',
                '_key': key,
                '_row': row,
                '_is_behoben': is_behoben
            })
        
        fehler_df = pd.DataFrame(fehler_data)
        
        st.dataframe(
            fehler_df[['Status', 'Name', 'Betrag', 'Zeit', 'Telefon', 'WhatsApp']],
            use_container_width=True,
            hide_index=True,
            height=min(len(fehler_df) * 35 + 38, 500)
        )
        
        st.markdown("---")
        st.markdown("### 🔧 Fehler bearbeiten")
        st.caption("Wähle einen Fehler aus, um Details zu sehen")
        
        fehler_options = [f"{f['Status']} {f['Name']} | {f['Betrag']} | {f['Zeit']}" for f in fehler_data]
        
        selected_fehler_name = st.selectbox(
            "Fehler auswählen:",
            options=fehler_options,
            key="fehler_selector"
        )
        
        selected_idx = fehler_options.index(selected_fehler_name)
        selected_fehler = fehler_data[selected_idx]
        
        row = selected_fehler['_row']
        key = selected_fehler['_key']
        is_behoben = selected_fehler['_is_behoben']
        whatsapp_sent_time = get_whatsapp_sent_time(row)
        
        st.markdown("---")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            st.markdown(f"**🧑** {row['Name']}")
            st.caption(f"⏰ {row.get('Service_Zeit', 'N/A')} | 💰 €{row['Betrag']} | 📅 {row['Datum']}")
            
            if whatsapp_sent_time:
                st.caption(f"✅ WhatsApp: {whatsapp_sent_time.strftime('%d.%m. %H:%M')}")
        
        with col2:
            customer_data = get_customer_data(row['Name'])
            
            if customer_data:
                phone = customer_data['phone_number']
                email = customer_data['email']
                cat = customer_data['category']
                
                st.caption(f"📱 {phone}")
                st.caption(f"📧 {email[:30]}..." if len(email) > 30 else f"📧 {email}")
                st.caption(f"🏷️ {cat}")
            else:
                st.caption("⚠️ Nicht im Sheet")
        
        with col3:
            if not is_behoben:
                if st.button("✅ Als behoben", key=f"fix_{key}", type="primary", use_container_width=True):
                    if not corr.empty and 'key' in corr.columns:
                        corr = corr[corr['key'] != key]
                    new_row_df = pd.DataFrame([{
                        'key': key,
                        'date': st.session_state.current_date,
                        'behoben': True,
                        'timestamp': datetime.now().isoformat()
                    }])
                    corr = pd.concat([corr, new_row_df], ignore_index=True)
                    savesheet(corr, "corrections")
                    st.success("✅")
                    time.sleep(0.5)
                    st.rerun()
            else:
                if st.button("🔄 Wieder öffnen", key=f"reopen_{key}", use_container_width=True):
                    if not corr.empty and 'key' in corr.columns:
                        corr = corr[corr['key'] != key]
                        savesheet(corr, "corrections")
                    st.success("🔄")
                    time.sleep(0.5)
                    st.rerun()
        
        if not is_behoben:
            st.markdown("---")
            
            render_name_matching_interface(row, ci_df, mapping, rejected_matches, fehler)
            
            st.markdown("---")



        col_wa, col_email = st.columns(2)

        with col_wa:
            col_wa_player, col_wa_test = st.columns(2)

            # 1) WhatsApp an Spieler
            with col_wa_player:
                button_label = "🔄 Erneut senden" if whatsapp_sent_time else "📱 WhatsApp senden"
                button_type = "secondary" if whatsapp_sent_time else "primary"

                if st.button(button_label, key=f"wa_{key}", type=button_type, use_container_width=True):
                    st.session_state[f"confirm_wa_{key}"] = True
                    st.session_state[f"confirm_wa_mode_{key}"] = "player"
                    st.rerun()

            # 2) Test-WhatsApp an Admin
            with col_wa_test:
                if st.button("🧪 Test WhatsApp", key=f"wa_test_{key}", use_container_width=True):
                    st.session_state[f"confirm_wa_{key}"] = True
                    st.session_state[f"confirm_wa_mode_{key}"] = "test"
                    st.rerun()

            # Bestätigungsdialog für beide Modi
            if st.session_state.get(f"confirm_wa_{key}", False):
                mode = st.session_state.get(f"confirm_wa_mode_{key}", "player")
                col_y, col_n = st.columns(2)

                label_yes = "✅ Ja, an Spieler!" if mode == "player" else "✅ Ja, Test an mich!"

                with col_y:
                    if st.button(label_yes, key=f"y_{key}", type="primary", use_container_width=True):
                        with st.spinner("Sende WhatsApp..."):
                            if mode == "player":
                                ok = send_wellpass_whatsapp_to_player(row)
                            else:
                                ok = send_wellpass_whatsapp_test(row)

                            if ok:
                                st.session_state[f"confirm_wa_{key}"] = False
                                st.session_state[f"confirm_wa_mode_{key}"] = None
                                time.sleep(1)
                                st.rerun()

                with col_n:
                    if st.button("❌ Abbrechen", key=f"n_{key}", use_container_width=True):
                        st.session_state[f"confirm_wa_{key}"] = False
                        st.session_state[f"confirm_wa_mode_{key}"] = None
                        st.rerun()

        with col_email:
            if st.button("📧 E-Mail", key=f"email_{key}", disabled=True, use_container_width=True):
                        st.info("Coming soon")

    else:
        st.success("✅ Keine offenen Wellpass-Fehler! 🎉")
    
    render_learned_matches_manager()
    
    customers_check = loadsheet("customers")
    if not customers_check.empty and 'name' in customers_check.columns:
        render_test_fehler_section()
    else:
        st.markdown("---")
        st.info("ℹ️ **Test:** Lade Customer-CSV!")

with tab2:
    st.subheader("📊 Monat")
    
    today = date.today()
    years = list(range(2024, today.year + 1))
    months = list(range(1, 13))
    month_names = {
        1: 'Januar', 2: 'Februar', 3: 'März', 4: 'April',
        5: 'Mai', 6: 'Juni', 7: 'Juli', 8: 'August',
        9: 'September', 10: 'Oktober', 11: 'November', 12: 'Dezember'
    }
    
    col1, col2 = st.columns(2)
    with col1:
        selected_year = st.selectbox("Jahr:", years, index=len(years)-1)
    with col2:
        selected_month = st.selectbox("Monat:", months, format_func=lambda x: month_names[x], index=today.month-1)
    
    first_day = date(selected_year, selected_month, 1)
    last_day = date(selected_year, selected_month, monthrange(selected_year, selected_month)[1])
    
    buchungen = loadsheet("buchungen")
    
    if buchungen.empty or 'analysis_date' not in buchungen.columns:
        st.info("📦 Keine Daten - CSVs hochladen!")
        st.stop()
    
    buchungen['date_obj'] = pd.to_datetime(buchungen['analysis_date'], errors='coerce').dt.date
    month_data = buchungen[(buchungen['date_obj'] >= first_day) & (buchungen['date_obj'] <= last_day)]
    
    if month_data.empty:
        st.warning(f"⚠️ Keine Daten für {month_names[selected_month]} {selected_year}")
        st.stop()
    
    total_buchungen = len(month_data)
    relevant_buchungen = len(month_data[month_data['Relevant'] == 'Ja'])
    fehler_gesamt = len(month_data[month_data['Fehler'] == 'Ja'])
    
    checkins = loadsheet("checkins")
    if not checkins.empty and 'analysis_date' in checkins.columns:
        checkins['date_obj'] = pd.to_datetime(checkins['analysis_date'], errors='coerce').dt.date
        month_checkins = checkins[(checkins['date_obj'] >= first_day) & (checkins['date_obj'] <= last_day)]
        
        if not month_checkins.empty:
            unique_daily_checkins = month_checkins.drop_duplicates(subset=['analysis_date', 'Name_norm'])
            wellpass_checkins_monat = len(unique_daily_checkins)
        else:
            wellpass_checkins_monat = 0
    else:
        wellpass_checkins_monat = 0
    
    revenue_month = get_revenue_from_raw(start_date=first_day, end_date=last_day)
    wellpass_revenue_monat = wellpass_checkins_monat * WELLPASS_WERT
    gesamt_umsatz = revenue_month['gesamt'] + wellpass_revenue_monat
    
    st.markdown("---")
    st.markdown(f"### 📅 {month_names[selected_month]} {selected_year}")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("💰 Gesamt", f"€{gesamt_umsatz:.2f}")
    with col2:
        st.metric("🎾 Playtomic", f"€{revenue_month['gesamt']:.2f}")
    with col3:
        st.metric("💳 Wellpass", f"€{wellpass_revenue_monat:.2f}", f"{wellpass_checkins_monat} CI")
    with col4:
        st.metric("📊 Buchungen", f"{total_buchungen}")
    with col5:
        st.metric("🎯 Relevant", f"{relevant_buchungen}")
    with col6:
        fehler_rate = (fehler_gesamt/relevant_buchungen*100) if relevant_buchungen > 0 else 0
        st.metric("❌ Fehler", f"{fehler_gesamt}", f"{fehler_rate:.1f}%")
    
    st.markdown("---")
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.markdown("#### 💰 Umsatz")
        extras = revenue_month['baelle'] + revenue_month['schlaeger']
        
        fig = go.Figure(data=[go.Pie(
            labels=['Courts', 'Matches', 'Extras', 'Wellpass'],
            values=[revenue_month['reservierung'], revenue_month['open_match'], extras, wellpass_revenue_monat],
            hole=.4,
            marker_colors=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
        )])
        fig.update_layout(height=350, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    with col_chart2:
        st.markdown("#### 📊 Fehlerquote")
        
        korrekt = relevant_buchungen - fehler_gesamt
        
        fig = go.Figure(data=[go.Pie(
            labels=['Korrekt', 'Fehler'],
            values=[korrekt, fehler_gesamt],
            hole=.4,
            marker_colors=['#96CEB4', '#FF6B6B']
        )])
        fig.update_layout(height=350, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.markdown("#### 📈 Verlauf")
    
    daily_stats = []
    for single_date in pd.date_range(first_day, last_day):
        day_str = single_date.strftime("%Y-%m-%d")
        day_data = month_data[month_data['analysis_date'] == day_str]
        
        if not day_data.empty:
            daily_fehler = len(day_data[day_data['Fehler'] == 'Ja'])
            daily_relevant = len(day_data[day_data['Relevant'] == 'Ja'])
            
            if not checkins.empty:
                day_checkins = checkins[checkins['analysis_date'] == day_str]
                unique_day_checkins = day_checkins.drop_duplicates(subset=['Name_norm'])
                daily_wellpass = len(unique_day_checkins)
            else:
                daily_wellpass = 0
            
            daily_stats.append({
                'Datum': single_date.date(),
                'Fehler': daily_fehler,
                'Relevant': daily_relevant,
                'Wellpass': daily_wellpass
            })
    
    if daily_stats:
        daily_df = pd.DataFrame(daily_stats)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=daily_df['Datum'],
            y=daily_df['Relevant'],
            name='Relevant',
            marker_color='#4ECDC4'
        ))
        
        fig.add_trace(go.Bar(
            x=daily_df['Datum'],
            y=daily_df['Fehler'],
            name='Fehler',
            marker_color='#FF6B6B'
        ))
        
        fig.add_trace(go.Scatter(
            x=daily_df['Datum'],
            y=daily_df['Wellpass'],
            name='Wellpass',
            mode='lines+markers',
            line=dict(color='#96CEB4', width=3),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            barmode='group',
            height=400,
            xaxis_title="Datum",
            yaxis_title="Anzahl",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.markdown("#### 🔴 Top 5 Fehler-Tage")
    
    if daily_stats:
        top_fehler = sorted(daily_stats, key=lambda x: x['Fehler'], reverse=True)[:5]
        
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.markdown("**Datum**")
        with col2:
            st.markdown("**Fehler**")
        with col3:
            st.markdown("**Quote**")
        
        st.markdown("---")
        
        for i, day in enumerate(top_fehler):
            if day['Fehler'] > 0:
                quote = (day['Fehler']/day['Relevant']*100) if day['Relevant'] > 0 else 0
                
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    st.text(f"{i+1}. {day['Datum'].strftime('%d.%m.%Y')}")
                with col2:
                    st.text(f"{day['Fehler']}")
                with col3:
                    st.text(f"{quote:.1f}%")
    
    st.markdown("---")
    
    with st.expander("📋 Details", expanded=False):
        if not month_data.empty:
            display_data = month_data[['Datum', 'Name', 'Betrag', 'Service_Zeit', 'Check-in', 'Fehler']].copy()
            display_data = display_data.sort_values(['Datum', 'Name'])
            
            styled = display_data.style.map(color_fehler, subset=['Fehler'])
            st.dataframe(styled, use_container_width=True, hide_index=True, height=600)
            
            st.caption(f"📊 {len(display_data)} Einträge im {month_names[selected_month]} {selected_year}")
        else:
            st.info("Keine Daten")
    
    st.markdown("---")
    
    st.markdown("### 📝 Zusammenfassung")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        **📊 Statistiken:**
        - Gesamt: €{gesamt_umsatz:.2f}
        - Playtomic: €{revenue_month['gesamt']:.2f}
        - Wellpass: €{wellpass_revenue_monat:.2f}
        - Courts: €{revenue_month['reservierung']:.2f}
        - Matches: €{revenue_month['open_match']:.2f}
        - Extras: €{extras:.2f}
        """)
    
    with col2:
        fehler_rate_final = (fehler_gesamt/relevant_buchungen*100) if relevant_buchungen > 0 else 0
        erfolgsquote = 100 - fehler_rate_final
        
        st.markdown(f"""
        **✅ Performance:**
        - Buchungen: {total_buchungen}
        - Relevant: {relevant_buchungen}
        - Wellpass Check-ins: {wellpass_checkins_monat}
        - Fehler: {fehler_gesamt}
        - Fehlerquote: {fehler_rate_final:.1f}%
        - Erfolgsquote: {erfolgsquote:.1f}%
        """)

# ========================================
# FOOTER
# ========================================

st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.markdown(
        '<div style="text-align: center; color: #666; font-size: 12px;">'
        '🎾 <b>Padel Port Dashboard v20.1 FINAL</b><br>'
        '🚢 <b>Dock In. Game On.</b><br>'
        'Made with ❤️ | 🍪 Cookie-Login | 🔄 Smart Duplikat-Filter | 📊 Synchronisierte Navigation'
        '</div>', 
        unsafe_allow_html=True
    )
