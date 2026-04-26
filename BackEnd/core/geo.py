import re

# ISO 3166-2:BD District Mappings (Standard for WooCommerce BD)
BD_DISTRICTS = {
    "BD-01": "Bandarban",
    "BD-02": "Barguna",
    "BD-03": "Bogura",
    "BD-04": "Brahmanbaria",
    "BD-05": "Bagerhat",
    "BD-06": "Barishal",
    "BD-07": "Bhola",
    "BD-08": "Cumilla",
    "BD-09": "Chandpur",
    "BD-10": "Chattogram",
    "BD-11": "Cox's Bazar",
    "BD-12": "Chuadanga",
    "BD-13": "Dhaka",
    "BD-14": "Dinajpur",
    "BD-15": "Faridpur",
    "BD-16": "Feni",
    "BD-17": "Gopalganj",
    "BD-18": "Gazipur",
    "BD-19": "Gaibandha",
    "BD-20": "Habiganj",
    "BD-21": "Jamalpur",
    "BD-22": "Jashore",
    "BD-23": "Jhenaidah",
    "BD-24": "Joypurhat",
    "BD-25": "Jhalokathi",
    "BD-26": "Kishoreganj",
    "BD-27": "Khulna",
    "BD-28": "Kurigram",
    "BD-29": "Khagrachhari",
    "BD-30": "Kushtia",
    "BD-31": "Lakshmipur",
    "BD-32": "Lalmonirhat",
    "BD-33": "Manikganj",
    "BD-34": "Mymensingh",
    "BD-35": "Munshiganj",
    "BD-36": "Madaripur",
    "BD-37": "Magura",
    "BD-38": "Moulvibazar",
    "BD-39": "Meherpur",
    "BD-40": "Narayanganj",
    "BD-41": "Netrakona",
    "BD-42": "Narsingdi",
    "BD-43": "Narail",
    "BD-44": "Natore",
    "BD-45": "Chapai Nawabganj",
    "BD-46": "Nilphamari",
    "BD-47": "Noakhali",
    "BD-48": "Naogaon",
    "BD-49": "Pabna",
    "BD-50": "Pirojpur",
    "BD-51": "Patuakhali",
    "BD-52": "Panchagarh",
    "BD-53": "Rajbari",
    "BD-54": "Rajshahi",
    "BD-55": "Rangpur",
    "BD-56": "Rangamati",
    "BD-57": "Sherpur",
    "BD-58": "Satkhira",
    "BD-59": "Sirajganj",
    "BD-60": "Sylhet",
    "BD-61": "Sunamganj",
    "BD-62": "Shariatpur",
    "BD-63": "Tangail",
    "BD-64": "Thakurgaon"
}

# Key areas in Dhaka for sub-district refinement
DHAKA_AREAS = [
    "Uttara", "Gulshan", "Dhanmondi", "Banani", "Mirpur", "Mohammadpur", 
    "Motijheel", "Bashundhara", "Badda", "Rampura", "Jatrabari", "Farmgate",
    "Khilgaon", "Malibagh", "Moghbazar", "Tongi", "Savar", "Old Dhaka", "Keraniganj"
]

def clean_geo_name(input_str: str) -> str:
    """Removes ISO codes like BD-** and extra whitespace."""
    if not input_str: return "Unknown"
    # Remove BD-** codes
    clean = re.sub(r'BD-\d{2}', '', str(input_str), flags=re.IGNORECASE)
    # Remove commas and common artifacts
    clean = clean.replace(',', '').strip()
    return clean if clean else "Unknown"

def get_parent_district(district_code_or_name: str) -> str:
    """Returns the cleaned, standard parent district name for mapping."""
    if not district_code_or_name: return "Unknown"
    
    dist_str = str(district_code_or_name).strip().upper()
    
    # 1. Handle numeric strings or single codes (WooCommerce standard)
    # E.g., "13" -> "BD-13", "8" -> "BD-08"
    if dist_str.isdigit():
        dist_str = f"BD-{int(dist_str):02d}"
    elif len(dist_str) <= 2 and dist_str.isalnum():
        dist_str = f"BD-{dist_str.zfill(2)}"
        
    return BD_DISTRICTS.get(dist_str, clean_geo_name(dist_str))

def get_region_display(city: str, district: str) -> str:
    """
    Main logic for geographic intelligence.
    Converts ISO codes to names and refines Dhaka into specific areas.
    """
    city_str = str(city).strip() if city else ""
    dist_str = str(district).strip().upper() if district else ""
    
    # 1. Resolve District Name from Code
    district_name = get_parent_district(dist_str)
    
    # 2. Refinement Logic for Dhaka
    if district_name.lower() == "dhaka":
        # Check if city/area name is prominent
        for area in DHAKA_AREAS:
            if area.lower() in city_str.lower():
                return f"{area}, Dhaka"
        # If city contains useful info, use it, else just Dhaka
        if city_str and city_str.lower() != "dhaka":
            return f"{city_str.title()}, Dhaka"
        return "Dhaka City"
    
    # 3. Standard formatting (City, District) if they differ
    if city_str and city_str.lower() != district_name.lower():
        clean_city = clean_geo_name(city_str).title()
        if clean_city != "Unknown" and clean_city.lower() != district_name.lower():
            # If city is already in district_name, don't repeat
            if clean_city.lower() in district_name.lower():
                return district_name
            return f"{clean_city}, {district_name}"
            
    return district_name


# -----------------------------------------------------------------------------
# MERGED FROM zones.py & data.py
# -----------------------------------------------------------------------------

KNOWN_ZONES = [
    # --- Dhaka City & Periphery ---
    "Adabor", "Agargaon", "Aftabnagar", "Badda", "Merul Badda", "Middle Badda",
    "South Badda", "North Badda", "Bailey Road", "Banani", "Banglamotor",
    "Bangshal", "Baridhara", "Baridhara DOHS", "Bashaboo", "Bashundhara",
    "Bashundhara R/A", "Bawnia", "Berybidh", "Bimanbandar", "Bijoy Sarani",
    "Bosila", "Cantonment", "Chakbazar", "Changkharpool", "Chawkbazar",
    "Dakshinkhan", "Darus Salam", "Demra", "Dhanmondi", "Dolaikhal",
    "Doyaganj", "Elephant Road", "Eskaton", "Farmgate", "Fakirapool",
    "Gandaria", "Gendaria", "Gabtoli", "Goran", "Green Road", "Gulistan",
    "Gulshan", "Gulshan-1", "Gulshan-2", "Hazaribagh", "Hatirpool",
    "Hatirjnill", "Ibrahimpur", "Islampur", "Jatrabari", "Jurain",
    "Kadamtali", "Kafrul", "Kalabagan", "Kallyanpur", "Kamalapur",
    "Kamarpara", "Kamrangirchar", "Kathalbagan", "Kawran Bazar", "Kazipara",
    "Keraniganj", "Khilgaon", "Khilkhet", "Kotwali", "Kuril", "Lalbagh",
    "Lalmatia", "Malibagh", "Maniknagar", "Mandarina", "Munsiganj",
    "Matuail", "Mirpur", "Mirpur DOHS", "Mirpur-1", "Mirpur-2", "Mirpur-10",
    "Mirpur-11", "Mirpur-12", "Mirpur-14", "Moghbazar", "Mohakhali",
    "Mohakhali DOHS", "Mohammadpur", "Mohammedpur", "Motijheel", "Mugda",
    "Mugdapara", "Narayanganj", "Nawabganj", "New Eskaton", "New Market",
    "Niketon", "Nikunja", "Nilkhet", "Pallabi", "Paltan", "Panthapath",
    "Paribagh", "Puran Dhaka", "Postogola", "Purana Paltan", "Raja Bazar",
    "Rajarbagh", "Ramna", "Rampura", "Rayerbagh", "Rayer Bazar", "Rupnagar",
    "Sabujbagh", "Sadarghat", "Sangsad Bhaban", "Satarkul", "Segunbagicha",
    "Shah Ali", "Shahbag", "Shahjahanpur", "Shajahanpur", "Shampur",
    "Shantinagar", "Sher-e-Bangla Nagar", "Shewrapara", "Shiddheswari",
    "Shyampur", "Siddhesuree", "Sutrapur", "Tejgaon", "Tejgaon I/A",
    "Tikatuli", "Tongi", "Turag", "Uttar Khan", "Uttara", "Vatara", "Wari",
    "Zigatola", "Savar", "Ashulia", "Dhamrai", "Hemayetpur", "EPZ",
    
    # --- Chittagong (Chattogram) ---
    "Agrabad", "Akbar Shah", "Anderkilla", "Bakalia", "Bandar", "Bayazid",
    "Boalkhali", "Chandgaon", "Chawkbazar", "Chittagong Cantonment",
    "Double Mooring", "EPZ", "Halishahar", "Hathazari", "Jamalkhan",
    "Karnafuli", "Khulshi", "Kotwali", "Lalkhan Bazar", "Muradpur",
    "Nasirabad", "New Market", "Oxygen", "Pahartali", "Panchlaish",
    "Patenga", "Patiya", "Raozan", "Sadarghat", "Sitakunda", "WASA", "GEC",
    
    # --- Rajshahi ---
    "Boalia", "Chandrima", "Katakhali", "Motiher", "Rajpara", "Shah Makhdum",
    "Rajshahi Sadar", "Paba",
    
    # --- Khulna ---
    "Daulatpur", "Khalishpur", "Khan Jahan Ali", "Khulna Sadar", "Sonadanga",
    "Boyra", "Gollamari",
    
    # --- Sylhet ---
    "Ambarkhana", "Airport", "Bandar Bazar", "Jalalabad", "Kotwali",
    "Moglabazar", "Osmani Nagar", "Shah Paran", "South Surma", "Sylhet Sadar",
    "Zindabazar", "Uposhahar",
    
    # --- Barisal ---
    "Agailjhara", "Babuganj", "Bakerganj", "Banaripara", "Barisal Sadar",
    "Gournadi", "Hizla", "Mehendiganj", "Muladi", "Wazirpur",
    
    # --- Rangpur ---
    "Badarganj", "Gangachara", "Kaunia", "Mithapukur", "Pirgacha", "Pirganj",
    "Rangpur Sadar", "Taraganj",
    
    # --- Mymensingh ---
    "Bhaluka", "Dhobaura", "Fulbaria", "Gaffargaon", "Gauripur", "Haluaghat",
    "Ishwarganj", "Mymensingh Sadar", "Muktagacha", "Nandail", "Phulpur",
    "Trishal",
    
    # --- Cumilla (Comilla) ---
    "Barura", "Brahmanpara", "Burichang", "Chandina", "Chauddagram",
    "Cumilla Sadar", "Cumilla Sadar Dakshin", "Daudkandi", "Debidwar",
    "Homna", "Laksam", "Lalmai", "Meghna", "Monohargonj", "Muradnagar",
    "Nangalkot", "Titas", "Kandirpar", "Tomson Bridge", "Police Line",
    "Race Course",
    
    # --- Gazipur ---
    "Gazipur Sadar", "Kaliakair", "Kaliganj", "Kapasia", "Sreepur", "Tongi",
    "Board Bazar", "Chowrasta", "Joydebpur", "Konabari",
    
    # --- Narayanganj ---
    "Araihazar", "Bandar", "Narayanganj Sadar", "Rupganj", "Sonargaon",
    "Siddhirganj", "Fatullah", "Chashara",
    
    # --- Bogura (Bogra) ---
    "Adamdighi", "Bogura Sadar", "Dhunat", "Dhupchanchia", "Gabtali",
    "Kahaloo", "Nandigram", "Sariakandi", "Sherpur", "Shibganj", "Sonatala",
    
    # --- Generic Terms ---
    "Kotwali", "Sadar", "Pourashava", "Municipality",
]

def normalize_city_name(city_name):
    if not city_name:
        return ""
    c = str(city_name).strip().lower()
    if "brahmanbaria" in c: return "B. Baria"
    if "narsingdi" in c or "narsinghdi" in c: return "Narshingdi"
    if "bagura" in c or "bogura" in c: return "Bogra"
    if "chattogram" in c: return "Chittagong"
    if "cox" in c and "bazar" in c: return "Cox's Bazar"
    if "barishal" in c: return "Barisal"
    if "jashore" in c: return "Jessore"
    if "cumilla" in c: return "Comilla"
    return str(city_name).strip().title()

def extract_best_zone(address, known_zones=None):
    if known_zones is None:
        known_zones = KNOWN_ZONES
    if not isinstance(address, str) or not address:
        return ""
    addr_l = address.lower()
    matches = [z for z in known_zones if z.lower() in addr_l]
    if not matches:
        return ""
    matches.sort(key=len, reverse=True)
    return matches[0]

def format_address_logic(raw_addr, city_norm, extracted_zone, raw_city_val):
    addr = " ".join(str(raw_addr).split()).title()
    if raw_city_val and city_norm and str(raw_city_val).lower() != city_norm.lower():
        addr = re.compile(re.escape(str(raw_city_val)), re.IGNORECASE).sub(city_norm, addr)
    parts = [p.strip() for p in re.split(r"[,;]\s*", addr) if p.strip()]
    cleaned = []
    seen = set()
    for p in parts:
        pl = p.lower()
        if (
            pl in seen
            or (city_norm and pl == city_norm.lower())
            or (extracted_zone and pl == extracted_zone.lower())
        ):
            continue
        cleaned.append(p)
        seen.add(pl)
    if extracted_zone and (extracted_zone.lower() not in ["sadar", "city"] or not cleaned):
        if not any(extracted_zone.lower() in p.lower() for p in cleaned):
            cleaned.append(extracted_zone)
    if city_norm:
        cleaned.append(city_norm)
    return ", ".join(cleaned)
