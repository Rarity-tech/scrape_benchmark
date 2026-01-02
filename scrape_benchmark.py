#!/usr/bin/env python3
"""
=============================================================================
AIRBNB BENCHMARK SCRAPER
=============================================================================
Scrape tous les listings d'une zone avec leurs notes détaillées et badges
pour comparer et benchmarker les performances.
=============================================================================
"""

import os
import csv
import time
import json
import math
from datetime import datetime, timedelta
from curl_cffi import requests as curl_requests
import pyairbnb

# ==============================================================================
# CONFIGURATION (depuis les variables d'environnement)
# ==============================================================================

# Zone géographique
CENTER_LAT = float(os.environ.get("CENTER_LAT", "25.1950"))
CENTER_LNG = float(os.environ.get("CENTER_LNG", "55.2700"))
RADIUS_KM = float(os.environ.get("RADIUS_KM", "1.5"))

# Filtres (vide = pas de filtre)
ROOM_TYPE = os.environ.get("ROOM_TYPE", "").strip()
MIN_BEDROOMS = os.environ.get("MIN_BEDROOMS", "").strip()
MAX_BEDROOMS = os.environ.get("MAX_BEDROOMS", "").strip()
GUESTS = os.environ.get("GUESTS", "").strip()

# Dates
DAYS_FROM_NOW = int(os.environ.get("DAYS_FROM_NOW", "7"))
STAY_DURATION = int(os.environ.get("STAY_DURATION", "3"))

# Options
CURRENCY = os.environ.get("CURRENCY", "AED")

# Constantes
AIRBNB_API_KEY = "d306zoyjsyarp7ifhu67rjxn52tv0t20"
DELAY_BETWEEN_REQUESTS = 1.0
DELAY_BETWEEN_DETAILS = 0.5

# ==============================================================================
# UTILITAIRES
# ==============================================================================

def calculate_bounding_box(center_lat, center_lng, radius_km):
    """
    Calcule les coordonnées du rectangle (bounding box) à partir d'un point central et d'un rayon.
    """
    # 1 degré de latitude ≈ 111 km
    # 1 degré de longitude ≈ 111 km * cos(latitude)
    lat_offset = radius_km / 111.0
    lng_offset = radius_km / (111.0 * math.cos(math.radians(center_lat)))
    
    ne_lat = center_lat + lat_offset
    ne_lng = center_lng + lng_offset
    sw_lat = center_lat - lat_offset
    sw_lng = center_lng - lng_offset
    
    return {
        "ne_lat": ne_lat,
        "ne_lng": ne_lng,
        "sw_lat": sw_lat,
        "sw_lng": sw_lng
    }


def calculate_zoom_from_radius(radius_km):
    """
    Estime le niveau de zoom approprié basé sur le rayon.
    """
    if radius_km <= 0.5:
        return 16
    elif radius_km <= 1:
        return 15
    elif radius_km <= 2:
        return 14
    elif radius_km <= 5:
        return 13
    elif radius_km <= 10:
        return 12
    else:
        return 11


# ==============================================================================
# RECHERCHE API (indépendante de pyairbnb)
# ==============================================================================

def search_listings(check_in, check_out, bounds, zoom, filters):
    """
    Recherche Airbnb avec pagination complète et filtres.
    """
    url = "https://www.airbnb.com/api/v2/explore_tabs"
    
    # Paramètres de base
    base_params = {
        # Dates
        "checkin": check_in,
        "checkout": check_out,
        
        # Coordonnées
        "ne_lat": str(bounds["ne_lat"]),
        "ne_lng": str(bounds["ne_lng"]),
        "sw_lat": str(bounds["sw_lat"]),
        "sw_lng": str(bounds["sw_lng"]),
        "search_by_map": "true",
        "zoom": str(zoom),
        
        # Paramètres de recherche
        "version": "1.8.3",
        "satori_version": "1.2.0",
        "_format": "for_explore_search_web",
        "items_per_grid": "50",
        "screen_size": "large",
        
        # Autres
        "currency": CURRENCY,
        "locale": "en",
        "key": AIRBNB_API_KEY,
        "timezone_offset": "240",
        
        # Flags
        "is_guided_search": "true",
        "is_standard_search": "true",
        "refinement_paths[]": "/homes",
        "tab_id": "home_tab",
        "channel": "EXPLORE",
        "date_picker_type": "calendar",
        "source": "structured_search_input_header",
        "search_type": "user_map_move",
    }
    
    # Ajouter les filtres si présents
    if filters.get("adults"):
        base_params["adults"] = str(filters["adults"])
        base_params["children"] = "0"
        base_params["infants"] = "0"
    
    if filters.get("room_type"):
        room_type_map = {
            "entire_home": "Entire home/apt",
            "private_room": "Private room",
            "shared_room": "Shared room"
        }
        if filters["room_type"] in room_type_map:
            base_params["room_types[]"] = room_type_map[filters["room_type"]]
    
    if filters.get("min_bedrooms"):
        base_params["min_bedrooms"] = str(filters["min_bedrooms"])
    
    if filters.get("max_bedrooms"):
        base_params["max_bedrooms"] = str(filters["max_bedrooms"])
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Airbnb-API-Key": AIRBNB_API_KEY,
    }
    
    all_listings = []
    items_offset = 0
    section_offset = 0
    page_count = 0
    max_pages = 20  # Max ~1000 listings
    
    print(f"\n🔍 Recherche en cours...", flush=True)
    
    try:
        while page_count < max_pages:
            page_count += 1
            
            params = base_params.copy()
            if page_count > 1:
                params["items_offset"] = str(items_offset)
                params["section_offset"] = str(section_offset)
            
            response = curl_requests.get(
                url,
                params=params,
                headers=headers,
                impersonate="chrome120",
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"   ⚠️ HTTP {response.status_code}", flush=True)
                break
            
            data = response.json()
            page_listings = []
            pagination_metadata = None
            
            explore_tabs = data.get("explore_tabs", [])
            
            for tab in explore_tabs:
                if not pagination_metadata:
                    pagination_metadata = tab.get("pagination_metadata", {})
                
                sections = tab.get("sections", [])
                for section in sections:
                    listings = section.get("listings", [])
                    
                    for listing in listings:
                        listing_data = listing.get("listing", {})
                        pricing = listing.get("pricing_quote", {})
                        
                        room_id = listing_data.get("id")
                        if not room_id:
                            continue
                        
                        # Éviter les doublons
                        if any(l["room_id"] == str(room_id) for l in all_listings):
                            continue
                        
                        # Extraire le prix
                        price = None
                        if pricing:
                            rate = pricing.get("rate", {})
                            amount = rate.get("amount")
                            if amount:
                                try:
                                    price = float(amount)
                                except:
                                    pass
                        
                        # Extraire les infos de base
                        page_listings.append({
                            "room_id": str(room_id),
                            "name": listing_data.get("name", ""),
                            "room_type": listing_data.get("room_type", ""),
                            "person_capacity": listing_data.get("person_capacity", ""),
                            "bedrooms": listing_data.get("bedrooms", ""),
                            "beds": listing_data.get("beds", ""),
                            "bathrooms": listing_data.get("bathrooms", ""),
                            "price": price,
                            "is_superhost": listing_data.get("is_superhost", False),
                            "avg_rating": listing_data.get("avg_rating", ""),
                            "reviews_count": listing_data.get("reviews_count", ""),
                            # Badges depuis les données de recherche
                            "guest_favorite": listing_data.get("is_guest_favorite", False),
                        })
            
            all_listings.extend(page_listings)
            
            print(f"   📄 Page {page_count}: +{len(page_listings)} listings (total: {len(all_listings)})", flush=True)
            
            if not page_listings:
                break
            
            has_next_page = pagination_metadata.get("has_next_page", False) if pagination_metadata else False
            
            if not has_next_page:
                break
            
            items_offset = pagination_metadata.get("items_offset", 0) if pagination_metadata else 0
            section_offset = pagination_metadata.get("section_offset", 0) if pagination_metadata else 0
            
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        print(f"\n✅ Total trouvé: {len(all_listings)} listings", flush=True)
        return all_listings
        
    except Exception as e:
        print(f"❌ Erreur recherche: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return all_listings


# ==============================================================================
# RÉCUPÉRATION DES DÉTAILS (notes, badges)
# ==============================================================================

def get_listing_details(room_id):
    """
    Récupère les détails complets d'un listing via pyairbnb.
    """
    try:
        details = pyairbnb.get_details(
            room_id=room_id,
            currency=CURRENCY,
            proxy_url="",
            language="en",
        )
        return details
    except Exception as e:
        print(f"      ⚠️ Erreur détails {room_id}: {e}", flush=True)
        return None


def extract_ratings_and_badges(details):
    """
    Extrait les notes détaillées et badges depuis les détails du listing.
    """
    result = {
        "rating_overall": "",
        "rating_accuracy": "",
        "rating_cleanliness": "",
        "rating_checkin": "",
        "rating_communication": "",
        "rating_location": "",
        "rating_value": "",
        "reviews_count": "",
        "host_id": "",
        "host_name": "",
        "host_rating": "",
        "host_reviews_count": "",
        "is_superhost": False,
        "is_guest_favorite": False,
        "top_percent": "",
        "badges": "",
    }
    
    if not details:
        return result
    
    # Note globale
    result["rating_overall"] = details.get("review_details_interface", {}).get("review_score", "") or details.get("avg_rating", "")
    
    # Sous-notes
    review_summary = details.get("review_details_interface", {}).get("review_summary", [])
    if isinstance(review_summary, list):
        for item in review_summary:
            label = item.get("label", "").lower()
            value = item.get("localizedRating", "") or item.get("value", "")
            
            if "accuracy" in label:
                result["rating_accuracy"] = value
            elif "cleanliness" in label or "clean" in label:
                result["rating_cleanliness"] = value
            elif "check" in label or "checkin" in label:
                result["rating_checkin"] = value
            elif "communication" in label:
                result["rating_communication"] = value
            elif "location" in label:
                result["rating_location"] = value
            elif "value" in label:
                result["rating_value"] = value
    
    # Nombre d'avis
    result["reviews_count"] = details.get("visible_review_count", "") or details.get("review_count", "")
    
    # Host info
    host_data = details.get("host", {})
    if isinstance(host_data, dict):
        result["host_id"] = str(host_data.get("id", ""))
        result["host_name"] = host_data.get("name", "") or host_data.get("first_name", "")
        result["is_superhost"] = host_data.get("is_superhost", False)
        result["host_rating"] = host_data.get("host_rating", "")
        result["host_reviews_count"] = host_data.get("host_total_reviews_count", "")
    
    # Guest Favorite
    result["is_guest_favorite"] = details.get("is_guest_favorite", False) or details.get("guest_favorite", False)
    
    # Top X% - chercher dans plusieurs endroits possibles
    highlights = details.get("listing_highlights", [])
    if isinstance(highlights, list):
        for highlight in highlights:
            text = str(highlight).lower()
            if "top 1%" in text:
                result["top_percent"] = "1"
            elif "top 5%" in text:
                result["top_percent"] = "5"
            elif "top 10%" in text:
                result["top_percent"] = "10"
    
    # Badges divers
    badges_list = []
    if result["is_superhost"]:
        badges_list.append("Superhost")
    if result["is_guest_favorite"]:
        badges_list.append("Guest Favorite")
    if result["top_percent"]:
        badges_list.append(f"Top {result['top_percent']}%")
    
    # Chercher d'autres badges dans les highlights
    if isinstance(highlights, list):
        for highlight in highlights:
            if isinstance(highlight, dict):
                title = highlight.get("title", "")
                if title and title not in badges_list:
                    badges_list.append(title)
    
    result["badges"] = " | ".join(badges_list)
    
    return result


# ==============================================================================
# EXPORT CSV
# ==============================================================================

def export_to_csv(listings, filename):
    """
    Exporte les données vers un fichier CSV.
    """
    fieldnames = [
        "room_id",
        "url",
        "title",
        "room_type",
        "bedrooms",
        "beds",
        "bathrooms",
        "guests_capacity",
        "price",
        "rating_overall",
        "rating_accuracy",
        "rating_cleanliness",
        "rating_checkin",
        "rating_communication",
        "rating_location",
        "rating_value",
        "reviews_count",
        "host_id",
        "host_name",
        "host_rating",
        "host_reviews_count",
        "is_superhost",
        "is_guest_favorite",
        "top_percent",
        "badges",
    ]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for listing in listings:
            row = {
                "room_id": listing.get("room_id", ""),
                "url": f"https://www.airbnb.com/rooms/{listing.get('room_id', '')}",
                "title": listing.get("name", ""),
                "room_type": listing.get("room_type", ""),
                "bedrooms": listing.get("bedrooms", ""),
                "beds": listing.get("beds", ""),
                "bathrooms": listing.get("bathrooms", ""),
                "guests_capacity": listing.get("person_capacity", ""),
                "price": listing.get("price", ""),
                "rating_overall": listing.get("rating_overall", ""),
                "rating_accuracy": listing.get("rating_accuracy", ""),
                "rating_cleanliness": listing.get("rating_cleanliness", ""),
                "rating_checkin": listing.get("rating_checkin", ""),
                "rating_communication": listing.get("rating_communication", ""),
                "rating_location": listing.get("rating_location", ""),
                "rating_value": listing.get("rating_value", ""),
                "reviews_count": listing.get("reviews_count", ""),
                "host_id": listing.get("host_id", ""),
                "host_name": listing.get("host_name", ""),
                "host_rating": listing.get("host_rating", ""),
                "host_reviews_count": listing.get("host_reviews_count", ""),
                "is_superhost": listing.get("is_superhost", False),
                "is_guest_favorite": listing.get("is_guest_favorite", False),
                "top_percent": listing.get("top_percent", ""),
                "badges": listing.get("badges", ""),
            }
            writer.writerow(row)
    
    print(f"\n📁 Fichier créé: {filename}", flush=True)


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 80)
    print("🏠 AIRBNB BENCHMARK SCRAPER")
    print("=" * 80)
    
    # Calculer les dates
    check_in_date = datetime.now() + timedelta(days=DAYS_FROM_NOW)
    check_out_date = check_in_date + timedelta(days=STAY_DURATION)
    check_in = check_in_date.strftime("%Y-%m-%d")
    check_out = check_out_date.strftime("%Y-%m-%d")
    
    # Calculer la zone
    bounds = calculate_bounding_box(CENTER_LAT, CENTER_LNG, RADIUS_KM)
    zoom = calculate_zoom_from_radius(RADIUS_KM)
    
    # Préparer les filtres
    filters = {}
    if GUESTS:
        filters["adults"] = int(GUESTS)
    if ROOM_TYPE:
        filters["room_type"] = ROOM_TYPE
    if MIN_BEDROOMS:
        filters["min_bedrooms"] = int(MIN_BEDROOMS)
    if MAX_BEDROOMS:
        filters["max_bedrooms"] = int(MAX_BEDROOMS)
    
    # Afficher la configuration
    print(f"\n📍 ZONE:")
    print(f"   Centre: {CENTER_LAT}, {CENTER_LNG}")
    print(f"   Rayon: {RADIUS_KM} km")
    print(f"   Zoom: {zoom}")
    print(f"   Bounding box:")
    print(f"      NE: {bounds['ne_lat']:.6f}, {bounds['ne_lng']:.6f}")
    print(f"      SW: {bounds['sw_lat']:.6f}, {bounds['sw_lng']:.6f}")
    
    print(f"\n📅 DATES:")
    print(f"   Check-in: {check_in} (dans {DAYS_FROM_NOW} jours)")
    print(f"   Check-out: {check_out} ({STAY_DURATION} nuits)")
    
    print(f"\n🔧 FILTRES:")
    print(f"   Type: {ROOM_TYPE or '(tous)'}")
    print(f"   Chambres: {MIN_BEDROOMS or '?'} - {MAX_BEDROOMS or '?'}")
    print(f"   Voyageurs: {GUESTS or '(tous)'}")
    print(f"   Devise: {CURRENCY}")
    
    print("=" * 80)
    
    # Phase 1: Recherche
    print("\n📊 PHASE 1: RECHERCHE DES LISTINGS")
    print("-" * 40)
    
    listings = search_listings(check_in, check_out, bounds, zoom, filters)
    
    if not listings:
        print("\n❌ Aucun listing trouvé!")
        return
    
    # Phase 2: Récupération des détails
    print("\n📊 PHASE 2: RÉCUPÉRATION DES DÉTAILS")
    print("-" * 40)
    print(f"   {len(listings)} listings à traiter...\n")
    
    for idx, listing in enumerate(listings, start=1):
        room_id = listing["room_id"]
        print(f"   [{idx}/{len(listings)}] Room {room_id}...", end=" ", flush=True)
        
        details = get_listing_details(room_id)
        
        if details:
            ratings = extract_ratings_and_badges(details)
            listing.update(ratings)
            
            # Afficher un résumé
            rating = listing.get("rating_overall", "N/A")
            badges = listing.get("badges", "")
            print(f"✓ Rating: {rating} | {badges or 'Aucun badge'}", flush=True)
        else:
            print("⚠️ Pas de détails", flush=True)
        
        time.sleep(DELAY_BETWEEN_DETAILS)
    
    # Phase 3: Export
    print("\n📊 PHASE 3: EXPORT CSV")
    print("-" * 40)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_{timestamp}.csv"
    export_to_csv(listings, filename)
    
    # Résumé
    print("\n" + "=" * 80)
    print("🎉 TERMINÉ!")
    print("=" * 80)
    
    # Stats
    superhosts = sum(1 for l in listings if l.get("is_superhost"))
    guest_favorites = sum(1 for l in listings if l.get("is_guest_favorite"))
    top_1 = sum(1 for l in listings if l.get("top_percent") == "1")
    top_5 = sum(1 for l in listings if l.get("top_percent") == "5")
    top_10 = sum(1 for l in listings if l.get("top_percent") == "10")
    
    ratings = [float(l.get("rating_overall", 0)) for l in listings if l.get("rating_overall")]
    avg_rating = sum(ratings) / len(ratings) if ratings else 0
    
    print(f"\n📊 STATISTIQUES:")
    print(f"   Total listings: {len(listings)}")
    print(f"   Note moyenne: {avg_rating:.2f}")
    print(f"   Superhosts: {superhosts} ({100*superhosts/len(listings):.1f}%)")
    print(f"   Guest Favorites: {guest_favorites} ({100*guest_favorites/len(listings):.1f}%)")
    print(f"   Top 1%: {top_1}")
    print(f"   Top 5%: {top_5}")
    print(f"   Top 10%: {top_10}")
    
    print(f"\n📁 Fichier: {filename}")
    print("=" * 80)


if __name__ == "__main__":
    main()
