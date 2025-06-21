# bot_logic.py
# -*- coding: utf-8 -*-

import os
import logging
import time
import json 
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict
from urllib.parse import quote_plus

load_dotenv()

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
GOOGLE_SHEET_LOCAL_INFO_NAME = os.getenv("GOOGLE_SHEET_LOCAL_INFO_NAME", "Tiruchendur_Local_Info")
GOOGLE_SHEET_PARKING_LOTS_INFO_NAME = os.getenv("GOOGLE_SHEET_PARKING_LOTS_INFO", "Tiruchendur_Parking_Lots_Info")
GOOGLE_SHEET_PARKING_STATUS_LIVE_NAME = os.getenv("GOOGLE_SHEET_PARKING_STATUS_LIVE", "Tiruchendur_Parking_Status_Live")
credentials_filename = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "credentials.json")
GOOGLE_SHEETS_CREDENTIALS_FILE = os.path.join(BASE_DIR, credentials_filename)
GOOGLE_FORM_FEEDBACK_LINK = "https://docs.google.com/forms/d/e/1FAIpQLSempmuc0_3KkCX3JK3wCZTod51Zw3o8ZkG78kQpcMTmVTGsPg/viewform?usp=header"

# --- Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Constants & Menus ---
MENU_TEXTS = {
    "en": {
        "welcome_tiruchendur": "Vanakkam {user_name}! I'm your Tiruchendur Assistant. 😊",
        "select_language_prompt": "Please select your preferred language.",
        "invalid_language_selection": "Invalid selection. Please click one of the buttons.",
        "language_selected": "You have selected {language_name}.",
        "main_menu_prompt": "Tiruchendur Main Menu - Type the number for your choice:",
        "option_parking_availability": "1. 🅿️ Live Parking Availability",
        "option_temple_info": "2. Murugan Temple Info",
        "option_help_centres": "3. 'May I Help You?' Centres",
        "option_first_aid": "4. First Aid Stations",
        "option_temp_bus_stands": "5. Temporary Bus Stands",
        "option_toilets_temple": "6. Toilets Near Temple",
        "option_annadhanam": "7. Annadhanam Details",
        "option_emergency_contacts": "8. Emergency Helpline Numbers",
        "option_nearby_facilities": "9. Search Nearby (ATM, Hotel etc.)",
        "option_change_language": "10. Change Language",
        "option_feedback": "11. Feedback",
        "option_end_conversation_text": "\nType 'X' to End Conversation.",
        "feedback_response": "Thank you for helping us improve! 🙏\nPlease share your valuable feedback using the link below:\n\n<a href=\"{feedback_link}\" target=\"_blank\" rel=\"noopener noreferrer\">Open Feedback Form</a>",
        "invalid_menu_option": "Invalid option. Please type a number from the menu or 'X' to end.",
        "temple_info_menu_prompt": "Murugan Temple Information - Type the number:",
        "temple_timings_menu_item": "1. Nada Open/Close & Pooja Times",
        "temple_dress_code_menu_item": "2. Dress Code",
        "temple_seva_tickets_menu_item": "3. Seva & Ticket Details",
        "option_go_back_text": "0. Go Back to Main Menu",
        "freestyle_query_prompt": "Okay, what would you like to search for nearby (e.g., 'ATM', 'hotels', 'restaurants')?",
        "emergency_contacts_info": "Tiruchendur Emergency Contacts:\nPolice: 100\nFire: 101\nAmbulance: 108\nTemple Office: [Insert Number]",
        "local_info_title_format": "--- {category_name} in Tiruchendur ---",
        "local_info_item_format": "\n➡️ {ItemName}\n📍 Location: {LocationLink}\n📝 Notes: {Notes}",
        "local_info_item_format_bus": "\n➡️ {ItemName}\n🛣️ Route: {RouteInfo}\n📍 Location: {LocationLink}\n🕒 Active: {ActiveDuring}\n📝 Notes: {Notes}",
        "local_info_item_format_parking": "\n🅿️ {ItemName}\n🛣️ Access from: {RouteDirection}\n📍 Location: {LocationLink}\n🕒 Operation: {OperationDuring}\n📝 Notes: {Notes}",
        "local_info_item_format_annadhanam": "\n🍚 Annadhanam at: {ItemName}\n🗺️ Map: {MapsLink}\n🕒 Timings: {Timings}\n📞 Contact: {ContactInfo}\n📝 Notes: {Notes}",
        "no_local_info_found": "No information currently available for {category_name} in Tiruchendur.",
        "fetching_data_error": "Sorry, I couldn't fetch the latest information.",
        "parking_route_prompt": "Which route are you primarily arriving from for parking?\n(Type the number or name)\n1. Tirunelveli Route\n2. Thoothukudi Route\n3. Nagercoil Route\n4. Other/Already in Tiruchendur",
        "parking_for_route_title": "--- Parking Options for {RouteName} Route ---",
        "parking_info_title": "--- Tiruchendur Parking Availability ---",
        "no_parking_available": "Sorry, no suitable parking spots are currently available or all are nearly full.",
        "parking_lot_details_format": "\n🅿️ {ParkingName}\n🗺️ Directions: {MapsLink}\n📍 Approx. {Distance:.1f} km away\n📦 Availability: {Availability}/{TotalCapacity} slots ({PercentageFull:.0f}% full)",
        "overall_parking_map_link_text": "\n\n<a href=\"{overall_map_url}\" data-embed=\"true\">🗺️ View All Parking Lots for the {RouteName} Route</a>",
        "temple_timings_details": "Tiruchendur Murugan Temple General Timings:",
        "temple_dress_code_details": "Dress Code: Traditional Indian attire is recommended. Men: Dhoti/Pants. Women: Saree/Salwar Kameez.",
        "temple_seva_details_intro": "--- Seva & Ticket Details (Rates subject to change) ---",
        "goodbye_message": "Nandri! Vanakkam!",
        "nearest_place_intro": "📍 Here are results for {place_type_display_name} in the Tiruchendur area:",
        "place_details_maps": "\n{name}\nAddress: {address}\n🗺️ {maps_url}"
    },
    "ta": {
        # ... (Full Tamil translations are correct and unchanged) ...
    }
}
SUPPORTED_LANGUAGES = { "en": {"name": "English"}, "ta": {"name": "தமிழ் (Tamil)"} }
SHEET_HELP_CENTRES, SHEET_FIRST_AID, SHEET_TEMP_BUS_STANDS, SHEET_TOILETS, SHEET_DESIGNATED_PARKING_STATIC, SHEET_ANNADHANAM = "Help_Centres", "First_Aid_Stations", "Temp_Bus_Stands", "Toilets_Near_Temple", "Designated_Public_Parking", "Annadhanam_Details"
OVERALL_ROUTE_MY_MAPS = {"thoothukudi": "1RTKvzXANpeJXI5wsW28WGclXkO2T7kw", "tirunelveli": "1cROpQnVd_Jk7B6KPDyhreS98ek1GDrQ", "nagercoil": "17GYGNfx6r8bO7ORC7QfYgQHyF1gT2_4"}

class BotLogic:
    def __init__(self):
        logger.info("Initializing BotLogic...")
        self.user_states = {}
        self.TIRUCHENDUR_COORDS = (8.4967, 78.1245)
        self.LOCAL_INFO_CACHE, self.LAST_LOCAL_INFO_FETCH_TIME = {}, {}
        self.PARKING_LOTS_INFO_CACHE, self.LAST_PARKING_LOTS_INFO_FETCH_TIME = [], 0
        self.PARKING_LIVE_STATUS_CACHE, self.LAST_PARKING_LIVE_STATUS_FETCH_TIME = {}, 0
        self.LIVE_DATA_CACHE_DURATION, self.LOCAL_INFO_CACHE_DURATION, self.STATIC_DATA_CACHE_DURATION = 120, 600, 1800
        self.PARKING_FULL_THRESHOLD_PERCENT = 70.0
        self.gspread_client = None
        self._preload_data()

    def get_gspread_client(self, force_reauth=False):
        if self.gspread_client and not force_reauth:
            return self.gspread_client
        logger.info(f"Authorizing gspread client. Force re-auth: {force_reauth}")
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly']
        google_creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        try:
            if google_creds_json:
                creds_dict = json.loads(google_creds_json)
                creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            elif os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
                creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=scopes)
            else:
                logger.error("FATAL: No Google credentials found.")
                return None
            self.gspread_client = gspread.authorize(creds)
            logger.info("gspread client authorized successfully.")
            return self.gspread_client
        except Exception as e:
            logger.error(f"Gspread auth error: {e}", exc_info=True)
            self.gspread_client = None
            return None

    def _preload_data(self):
        """
        EFFICIENTLY pre-loads all data by opening each spreadsheet only once.
        This is crucial to avoid hitting Google API rate limits on Vercel.
        """
        client = self.get_gspread_client()
        if not client:
            logger.error("Could not authorize gspread client at startup. Data fetching will be disabled.")
            return

        logger.info("Pre-loading all data from Google Sheets at startup...")
        try:
            # --- BATCH 1: Local Info Sheet ---
            logger.info(f"Opening spreadsheet: {GOOGLE_SHEET_LOCAL_INFO_NAME}")
            local_info_spreadsheet = client.open(GOOGLE_SHEET_LOCAL_INFO_NAME)
            for ws in local_info_spreadsheet.worksheets():
                logger.info(f"Fetching data from tab: {ws.title}")
                self.LOCAL_INFO_CACHE[ws.title] = ws.get_all_records()
                self.LAST_LOCAL_INFO_FETCH_TIME[ws.title] = time.time()
            logger.info("Local info pre-loaded.")

            # --- BATCH 2: Parking Lots Info Sheet ---
            logger.info(f"Opening spreadsheet: {GOOGLE_SHEET_PARKING_LOTS_INFO_NAME}")
            parking_lots_sheet = client.open(GOOGLE_SHEET_PARKING_LOTS_INFO_NAME).worksheet("Sheet1")
            self.PARKING_LOTS_INFO_CACHE = parking_lots_sheet.get_all_records()
            self.LAST_PARKING_LOTS_INFO_FETCH_TIME = time.time()
            logger.info("Parking lots info pre-loaded.")

            # --- BATCH 3: Parking Live Status Sheet ---
            logger.info(f"Opening spreadsheet: {GOOGLE_SHEET_PARKING_STATUS_LIVE_NAME}")
            live_status_sheet = client.open(GOOGLE_SHEET_PARKING_STATUS_LIVE_NAME).worksheet("Sheet1")
            records = live_status_sheet.get_all_records()
            self.PARKING_LIVE_STATUS_CACHE = {str(r['ParkingLotID']): r for r in records if 'ParkingLotID' in r}
            self.LAST_PARKING_LIVE_STATUS_FETCH_TIME = time.time()
            logger.info("Parking live status pre-loaded.")

            logger.info("All data pre-loading complete.")
        except gspread.exceptions.APIError as e:
            logger.error(f"GSpread API Error during preload (Quota Exceeded?): {e}.")
        except Exception as e:
            logger.error(f"An unexpected error occurred during preload: {e}", exc_info=True)

    # ... The rest of the file (process_user_input, handlers, etc.) is correct and unchanged ...

    def _get_response_structure(self, text="", photos=None, buttons=None):
        return {"text": text, "photos": photos or [], "buttons": buttons or []}

    def process_user_input(self, user_id: str, input_type: str, data: Any, user_name: str = "User") -> Dict:
        if data == "start_session_command" or user_id not in self.user_states:
            self.user_states[user_id] = {"lang": "en", "menu_level": "language_select"}
            return self._change_language(user_id, is_initial=True, user_name=user_name)
        
        state = self.user_states[user_id]
        
        if state.get("menu_level") == "language_select":
            lang_choice = str(data).strip().lower()
            if lang_choice in SUPPORTED_LANGUAGES:
                state['lang'], state['menu_level'] = lang_choice, 'main_menu'
                welcome_text = self.get_text(user_id, "language_selected", language_name=SUPPORTED_LANGUAGES[lang_choice]['name'])
                return self._get_response_structure(f"{welcome_text}\n\n{self._get_menu_text('main_menu', user_id)}")
            else:
                return self._change_language(user_id, user_name=user_name)

        text_input = str(data).strip()
        if text_input.lower() == 'x':
            lang = state.get("lang", "en")
            if user_id in self.user_states:
                del self.user_states[user_id]
            return self._get_response_structure(self.get_text(lang, "goodbye_message"))

        handler = getattr(self, f"_handle_{state.get('menu_level', 'main_menu')}", self._handle_invalid_state)
        return handler(user_id, text_input)

    def _handle_invalid_state(self, user_id, text_input):
        self.user_states[user_id]["menu_level"] = "main_menu"
        response = self._get_response_structure(self.get_text(user_id, 'invalid_menu_option'))
        response['next_menu'] = 'main_menu'
        return response

    def _handle_main_menu(self, user_id, choice):
        menu_actions = {
            "1": ("parking_awaiting_route", None), "2": ("temple_info_menu", None),
            "3": (None, lambda: self._get_formatted_sheet_data(user_id, SHEET_HELP_CENTRES)),
            "4": (None, lambda: self._get_formatted_sheet_data(user_id, SHEET_FIRST_AID)),
            "5": (None, lambda: self._get_formatted_sheet_data(user_id, SHEET_TEMP_BUS_STANDS)),
            "6": (None, lambda: self._get_formatted_sheet_data(user_id, SHEET_TOILETS)),
            "7": (None, lambda: self._get_formatted_sheet_data(user_id, SHEET_ANNADHANAM)),
            "8": (None, lambda: self.get_text(user_id, "emergency_contacts_info")),
            "9": ("nearby_search", None), "10": (None, lambda: self._change_language(user_id)),
            "11": (None, lambda: self.get_text(user_id, "feedback_response", feedback_link=GOOGLE_FORM_FEEDBACK_LINK)),
        }
        new_level, action = menu_actions.get(choice, (None, None))

        if new_level:
            self.user_states[user_id]["menu_level"] = new_level
            prompt_map = {"parking_awaiting_route": "parking_route_prompt", "temple_info_menu": "temple_info_menu_prompt", "nearby_search": "freestyle_query_prompt"}
            return self._get_response_structure(self.get_text(user_id, prompt_map[new_level]))
        
        elif action:
            result = action()
            if isinstance(result, dict):
                result['next_menu'] = 'main_menu'
                return result
            response = self._get_response_structure(text=result)
            response['next_menu'] = 'main_menu'
            return response
        
        return self._handle_invalid_state(user_id, choice)

    def _handle_temple_info_menu(self, user_id, choice):
        if choice == "0": 
            self.user_states[user_id]["menu_level"] = "main_menu"
            return self._get_response_structure(self._get_menu_text("main_menu", user_id))

        response = self._get_response_structure()
        
        if choice == "1":
            response["text"] = self.get_text(user_id, "temple_timings_details")
            response["photos"] = ['assets/nadai_thirappu_neram.png', 'assets/pooja_vivaram.png']
        elif choice == "2":
            response["text"] = self.get_text(user_id, "temple_dress_code_details")
        elif choice == "3":
            response["text"] = self.get_text(user_id, "temple_seva_details_intro")
            response["photos"] = ['assets/sevai_kattanam.png']
        else:
            response["text"] = self.get_text(user_id, "invalid_menu_option")

        response['next_menu'] = 'temple_info_menu'
        return response

    def _handle_parking_awaiting_route(self, user_id, text_input):
        self.user_states[user_id]["menu_level"] = "main_menu"
        choice = text_input.lower()
        route_pref = "any"
        if "1" in choice or "tirunelveli" in choice: route_pref = "tirunelveli"
        elif "2" in choice or "thoothukudi" in choice: route_pref = "thoothukudi"
        elif "3" in choice or "nagercoil" in choice: route_pref = "nagercoil"
        parking_reply = self.find_available_parking(self.TIRUCHENDUR_COORDS[0], self.TIRUCHENDUR_COORDS[1], user_id, route_preference=route_pref)
        response = self._get_response_structure(text=parking_reply)
        response['next_menu'] = 'main_menu'
        return response

    def _handle_nearby_search(self, user_id, text_input):
        self.user_states[user_id]["menu_level"] = "main_menu"
        search_reply = self.find_nearby_place(self.TIRUCHENDUR_COORDS[0], self.TIRUCHENDUR_COORDS[1], text_input, user_id=user_id)
        response = self._get_response_structure(text=search_reply)
        response['next_menu'] = 'main_menu'
        return response

    def _change_language(self, user_id, is_initial=False, user_name="User"):
        self.user_states[user_id]['menu_level'] = 'language_select'
        text = (self.get_text("en", "welcome_tiruchendur", user_name=user_name) + "\n") if is_initial else ""
        text += self.get_text("en", "select_language_prompt")
        buttons = [{"text": d["name"], "payload": c} for c, d in SUPPORTED_LANGUAGES.items()]
        return self._get_response_structure(text=text, buttons=buttons)

    def get_text(self, lang_code, key, **kwargs):
        lang = lang_code if lang_code in SUPPORTED_LANGUAGES else self.user_states.get(lang_code, {}).get("lang", "en")
        template_string = MENU_TEXTS.get(lang, MENU_TEXTS["en"]).get(key, f"<{key}_MISSING>")
        if kwargs:
            try: return template_string.format(**kwargs)
            except KeyError as e:
                logger.error(f"Formatting failed for key '{key}'. Missing placeholder: {e}")
                return f"Error: Data for '{e}' is missing."
        return template_string

    def _get_menu_text(self, menu_type, user_id):
        keys = {
            "main_menu": ["main_menu_prompt", "option_parking_availability", "option_temple_info", "option_help_centres", "option_first_aid", "option_temp_bus_stands", "option_toilets_temple", "option_annadhanam", "option_emergency_contacts", "option_nearby_facilities", "option_change_language", "option_feedback", "option_end_conversation_text"], 
            "temple_info_menu": ["temple_info_menu_prompt", "temple_timings_menu_item", "temple_dress_code_menu_item", "temple_seva_tickets_menu_item", "option_go_back_text"]
        }.get(menu_type, [])
        return "\n".join([self.get_text(user_id, k) for k in keys])
        
    def _generate_embed_link(self, query: str = "", mode: str = "place", origin: str = "", destination: str = "", my_map_id: str = "") -> str:
        if my_map_id: return f"https://www.google.com/maps/d/embed?mid={my_map_id}"
        if not GOOGLE_MAPS_API_KEY: return ""
        base_url = "https://www.google.com/maps/embed/v1/"
        if mode == "place": url = f"{base_url}place?key={GOOGLE_MAPS_API_KEY}&q={quote_plus(query)}"
        elif mode == "directions": url = f"{base_url}directions?key={GOOGLE_MAPS_API_KEY}&origin={origin}&destination={destination}"
        elif mode == "search": url = f"{base_url}search?key={GOOGLE_MAPS_API_KEY}&q={quote_plus(query)}"
        else: return ""
        return url

    def _get_formatted_sheet_data(self, user_id: str, worksheet_name: str) -> str:
        data_items = self.LOCAL_INFO_CACHE.get(worksheet_name, [])
        if not data_items:
            logger.warning(f"Cache for {worksheet_name} is empty. Preload may have failed.")
            return self.get_text(user_id, "fetching_data_error")
        
        lang = self.user_states[user_id].get("lang", "en")
        format_map = {
            SHEET_HELP_CENTRES: ("option_help_centres", "local_info_item_format", "View Map"),
            SHEET_FIRST_AID: ("option_first_aid", "local_info_item_format", "View Map"),
            SHEET_TEMP_BUS_STANDS: ("option_temp_bus_stands", "local_info_item_format_bus", "View Map"),
            SHEET_TOILETS: ("option_toilets_temple", "local_info_item_format", "View Map"),
            SHEET_DESIGNATED_PARKING_STATIC: ("Designated Public Parking", "local_info_item_format_parking", "View Location"),
            SHEET_ANNADHANAM: ("option_annadhanam", "local_info_item_format_annadhanam", "View Map"),
        }
        category_key, item_format_key, link_text = format_map.get(worksheet_name, ("", "", ""))
        if not category_key: return "Error: Unknown data category."
        
        category_name = self.get_text(user_id, category_key).split('. ', 1)[-1] if '.' in self.get_text(user_id, category_key) else self.get_text(user_id, category_key)
        
        title = self.get_text(user_id, "local_info_title_format", category_name=category_name)
        reply_parts = [title]
        item_template = self.get_text(user_id, item_format_key)
        
        for item in data_items:
            format_kwargs = defaultdict(lambda: 'N/A', item)
            item_name = item.get(f'Name_{lang}', item.get('Name_en', 'N/A'))
            format_kwargs['ItemName'] = item_name
            for key_en in list(item.keys()):
                if key_en.endswith('_en'):
                    format_kwargs[key_en[:-3].capitalize()] = item.get(f"{key_en[:-3]}_{lang}", item.get(key_en, 'N/A'))
            embed_url = self._generate_embed_link(f"{item_name}, Tiruchendur")
            link_html = f'<a href="{embed_url}" data-embed="true">{link_text}</a>' if embed_url else "Map not available"
            format_kwargs["LocationLink"], format_kwargs["MapsLink"] = link_html, link_html
            reply_parts.append(item_template.format_map(format_kwargs))
        return "".join(reply_parts)
        
    def haversine(self, lat1, lon1, lat2, lon2):
        R = 6371; dLat, dLon = radians(lat2 - lat1), radians(lon2 - lon1)
        a = sin(dLat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dLon / 2)**2
        return R * 2 * atan2(sqrt(a), sqrt(1 - a))

    def find_available_parking(self, user_lat: float, user_lon: float, user_id: str, route_preference: Optional[str] = None) -> str:
        # Rely on pre-loaded cache. This makes the function much faster and less prone to API errors.
        if not self.PARKING_LOTS_INFO_CACHE or not self.PARKING_LIVE_STATUS_CACHE:
            logger.error("Parking data not available in cache. Preload may have failed.")
            return self.get_text(user_id, "fetching_data_error")
        
        current_lang = self.user_states[user_id].get("lang", "en")
        applicable_lots = [lot for lot in self.PARKING_LOTS_INFO_CACHE if (not route_preference or route_preference == "any" or route_preference.lower() in str(lot.get("Route_en", "any")).strip().lower())]

        if not applicable_lots:
            logger.warning(f"No parking lots found in sheet for route preference: {route_preference}")
            return self.get_text(user_id, "no_parking_available")

        processed_lots = []
        for lot in applicable_lots:
            try:
                lat, lon, capacity = float(lot.get('Latitude')), float(lot.get('Longitude')), int(lot.get('TotalCapacity', 0))
            except (ValueError, TypeError):
                logger.warning(f"Skipping lot due to invalid Lat/Lon/Capacity: {lot.get('Parking_name_en')}")
                continue
            if capacity <= 0: continue

            status = self.PARKING_LIVE_STATUS_CACHE.get(str(lot.get('ParkingLotID')), {})
            available = int(status.get('CurrentAvailability', -1))
            if available == -1: available = capacity - max(0, int(status.get('CurrentIn', 0)) - int(status.get('CurrentOut', 0)))
            percentage_full = ((capacity - available) / capacity * 100) if capacity > 0 else 100

            if available > 0 and percentage_full < self.PARKING_FULL_THRESHOLD_PERCENT:
                lot_data = lot.copy()
                lot_data.update({"Availability": available, "PercentageFull": percentage_full, "Distance": self.haversine(user_lat, user_lon, lat, lon), "Latitude": lat, "Longitude": lon, "TotalCapacity": capacity})
                processed_lots.append(lot_data)

        if not processed_lots: 
            logger.info(f"All lots for route {route_preference} are full or unavailable based on the threshold.")
            return self.get_text(user_id, "no_parking_available")

        sorted_lots = sorted(processed_lots, key=lambda x: (int(x.get('Priority', 99)), x['Distance']))
        
        title = self.get_text(user_id, "parking_for_route_title" if route_preference and route_preference != "any" else "parking_info_title", RouteName=route_preference.capitalize())
        
        details_list = []
        for lot in sorted_lots: # Show all available lots
            embed_url = self._generate_embed_link(mode="directions", origin=f"{user_lat},{user_lon}", destination=f"{lot['Latitude']},{lot['Longitude']}")
            maps_link = f'<a href="{embed_url}" data-embed="true">Get Directions</a>' if embed_url else "Directions unavailable"
            details_list.append(self.get_text(user_id, "parking_lot_details_format", ParkingName=lot.get(f"Parking_name_{current_lang}", lot.get("Parking_name_en")), Distance=lot['Distance'], Availability=lot['Availability'], TotalCapacity=lot['TotalCapacity'], PercentageFull=lot['PercentageFull'], MapsLink=maps_link))
        
        final_response = f"{title}\n" + "\n".join(details_list)

        if route_preference and route_preference in OVERALL_ROUTE_MY_MAPS:
            my_map_id = OVERALL_ROUTE_MY_MAPS[route_preference]
            overall_map_embed_url = self._generate_embed_link(my_map_id=my_map_id)
            final_response += self.get_text(user_id, "overall_parking_map_link_text", overall_map_url=overall_map_embed_url, RouteName=route_preference.capitalize())

        return final_response

    def find_nearby_place(self, lat: float, lon: float, search_query: str, user_id=None) -> str:
        place_type_display_name = search_query.replace('_', ' ').title()
        embed_url = self._generate_embed_link(f"{search_query} in Tiruchendur", mode="search", origin=f"{lat},{lon}")
        maps_url_html = f'<a href="{embed_url}" data-embed="true">View on Map</a>' if embed_url else "Map not available"
        
        return (f'{self.get_text(user_id, "nearest_place_intro", place_type_display_name=place_type_display_name)}'
                f'{self.get_text(user_id, "place_details_maps", name=f"Results for {place_type_display_name}", address="Click the link below to see locations on the map.", maps_url=maps_url_html)}')
