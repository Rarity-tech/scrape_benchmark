#!/usr/bin/env python3
"""
=============================================================================
AIRBNB BENCHMARK SCRAPER - VERSION CORRIGÉE
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
QUERY = os.environ.get("QUERY", "").strip()  # Ex: "Downtown Dubai"

# Filtres (vide = pas de filtre)
ROOM_TYPE = os.environ.get("ROOM_TYPE", "").strip()
MIN_BEDROOMS = os.environ.get("MIN_BEDROOMS", "").strip()
MAX_BEDROOMS = os.environ.get("MAX_BEDROOMS", "").strip()
GUESTS = os.environ.get("GUESTS", "").strip()
GUEST_FAVORITE = os.environ.get("GUEST_FAVORITE", "").strip().lower()
LUXE = os.environ.get("LUXE", "").strip().lower()

# Dates
DAYS_FROM_NOW = int(os.environ.get("DAYS_FROM_NOW", "7"))
STAY_DURATION = int(os.environ.get("STAY_DURATION", "3"))

# Options
CURRENCY = os.environ.get("CURRENCY", "AED")

# Constantes
AIRBNB_API_KEY = "d306zoyjsyarp7ifhu67rjxn52tv0t20"
DELAY_BETWEEN_REQUESTS = 1.0
DELAY_BETWEEN_DETAILS = 0.8

# ==============================================================================
# UTILITAIRES
# ==============================================================================

def calculate_bounding_box(center_lat, center_lng, radius_km):
    """
    Calcule les coordonnées du rectangle (bounding box) à partir d'un point central et d'un rayon.
    """
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
    Recherche Airbnb avec l'API GraphQL v3 StaysSearch.
    Supporte le filtre Guest Favorite natif.
    """
    
    # Hash SHA256 pour StaysSearch (capturé depuis le site Airbnb)
    GRAPHQL_HASH = "d9ab2c7e443b50fdce5cdcb69d4f7e7626dbab1609c981565a6c4bdbb04546e3"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "X-Airbnb-API-Key": AIRBNB_API_KEY,
        "X-Airbnb-GraphQL-Platform": "web",
        "X-Airbnb-GraphQL-Platform-Client": "minimalist-niobe",
        "X-CSRF-Without-Token": "1",
        "Origin": "https://www.airbnb.com",
        "Referer": "https://www.airbnb.com/s/homes",
    }
    
    def build_raw_params(cursor=None):
        """Construit les rawParams pour la requête GraphQL."""
        params = [
            {"filterName": "cdnCacheSafe", "filterValues": ["false"]},
            {"filterName": "channel", "filterValues": ["EXPLORE"]},
            {"filterName": "checkin", "filterValues": [check_in]},
            {"filterName": "checkout", "filterValues": [check_out]},
            {"filterName": "datePickerType", "filterValues": ["calendar"]},
            {"filterName": "flexibleTripLengths", "filterValues": ["one_week"]},
            {"filterName": "itemsPerGrid", "filterValues": ["50"]},
            {"filterName": "refinementPaths", "filterValues": ["/homes"]},
            {"filterName": "screenSize", "filterValues": ["large"]},
            {"filterName": "searchMode", "filterValues": ["regular_search"]},
            {"filterName": "tabId", "filterValues": ["home_tab"]},
            {"filterName": "version", "filterValues": ["1.8.3"]},
        ]
        
        # Toujours utiliser bounding box pour la zone
        params.append({"filterName": "neLat", "filterValues": [str(bounds["ne_lat"])]})
        params.append({"filterName": "neLng", "filterValues": [str(bounds["ne_lng"])]})
        params.append({"filterName": "swLat", "filterValues": [str(bounds["sw_lat"])]})
        params.append({"filterName": "swLng", "filterValues": [str(bounds["sw_lng"])]})
        params.append({"filterName": "searchByMap", "filterValues": ["true"]})
        
        # Filtres optionnels
        if filters.get("adults"):
            params.append({"filterName": "adults", "filterValues": [str(filters["adults"])]})
        
        if filters.get("room_type"):
            room_type_map = {
                "entire_home": "Entire home/apt",
                "private_room": "Private room",
                "shared_room": "Shared room"
            }
            if filters["room_type"] in room_type_map:
                params.append({"filterName": "roomTypes", "filterValues": [room_type_map[filters["room_type"]]]})
        
        if filters.get("min_bedrooms"):
            params.append({"filterName": "minBedrooms", "filterValues": [str(filters["min_bedrooms"])]})
        
        if filters.get("max_bedrooms"):
            params.append({"filterName": "maxBedrooms", "filterValues": [str(filters["max_bedrooms"])]})
        
        # ✅ Filtre Guest Favorite (natif API GraphQL)
        if filters.get("guest_favorite"):
            params.append({"filterName": "guestFavorite", "filterValues": ["true"]})
            params.append({"filterName": "selectedFilterOrder", "filterValues": ["guest_favorite:true"]})
        
        # Filtre Luxe
        if filters.get("luxe"):
            params.append({"filterName": "luxe", "filterValues": ["true"]})
            params.append({"filterName": "selectedFilterOrder", "filterValues": ["luxe:true"]})
        
        # Pagination
        if cursor:
            params.append({"filterName": "cursor", "filterValues": [cursor]})
        
        return params
    
    def build_graphql_payload(cursor=None):
        """Construit le payload complet pour la requête GraphQL."""
        raw_params = build_raw_params(cursor)
        
        # Treatment flags exacts du navigateur
        treatment_flags = [
            "feed_map_decouple_m11_treatment",
            "recommended_amenities_2024_treatment_b",
            "filter_redesign_2024_treatment",
            "filter_reordering_2024_roomtype_treatment",
            "p2_category_bar_removal_treatment",
            "selected_filters_2024_treatment",
            "recommended_filters_2024_treatment_b",
            "m13_search_input_phase2_treatment",
            "m13_search_input_services_enabled"
        ]
        
        search_request = {
            "metadataOnly": False,
            "requestedPageType": "STAYS_SEARCH",
            "searchType": "filter_change",
            "treatmentFlags": treatment_flags,
            "maxMapItems": 9999,
            "rawParams": raw_params
        }
        
        return {
            "operationName": "StaysSearch",
            "variables": {
                "staysSearchRequest": search_request,
                "staysMapSearchRequestV2": search_request.copy(),
                "isLeanTreatment": False,
                "aiSearchEnabled": False,
                "skipExtendedSearchParams": False
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": GRAPHQL_HASH
                }
            }
        }
    
    all_listings = []
    cursor = None
    page_count = 0
    max_pages = 20
    
    print(f"\n🔍 Recherche en cours (API GraphQL v3)...", flush=True)
    
    try:
        while page_count < max_pages:
            page_count += 1
            
            payload = build_graphql_payload(cursor)
            
            response = curl_requests.post(
                f"https://www.airbnb.com/api/v3/StaysSearch/{GRAPHQL_HASH}?operationName=StaysSearch&locale=en&currency={CURRENCY}",
                headers=headers,
                json=payload,
                impersonate="chrome120",
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"   ⚠️ HTTP {response.status_code}: {response.text[:200]}", flush=True)
                break
            
            data = response.json()
            
            # Vérifier les erreurs GraphQL
            if "errors" in data:
                print(f"   ⚠️ Erreur GraphQL: {data['errors']}", flush=True)
                break
            
            # Extraire les listings depuis la structure GraphQL
            page_listings = []
            
            stays_search = data.get("data", {}).get("presentation", {}).get("staysSearch", {})
            results = stays_search.get("results", {})
            search_results = results.get("searchResults", [])
            
            for result in search_results:
                # Structure GraphQL v3 : l'ID est dans demandStayListing.id (encodé base64)
                room_id = None
                dsl = result.get("demandStayListing", {})
                if dsl:
                    encoded_id = dsl.get("id", "")
                    if encoded_id:
                        try:
                            import base64
                            decoded = base64.b64decode(encoded_id).decode("utf-8")
                            # Format: "DemandStayListing:1149961985722988324"
                            if ":" in decoded:
                                room_id = decoded.split(":")[1]
                        except:
                            pass
                
                if not room_id:
                    continue
                
                # Éviter les doublons
                if any(l["room_id"] == str(room_id) for l in all_listings):
                    continue
                
                # Extraire le prix
                price = None
                structured_price = result.get("structuredDisplayPrice", {})
                if structured_price:
                    primary_line = structured_price.get("primaryLine", {})
                    # Peut être "discountedPrice" ou "price"
                    price = primary_line.get("discountedPrice") or primary_line.get("price")
                
                # Extraire rating depuis avgRatingLocalized (ex: "4.98 (42)")
                avg_rating = ""
                reviews_count = ""
                rating_str = result.get("avgRatingLocalized", "")
                if rating_str:
                    import re
                    match = re.match(r"([\d.]+)\s*\((\d+)\)", rating_str)
                    if match:
                        avg_rating = match.group(1)
                        reviews_count = match.group(2)
                    elif re.match(r"^[\d.]+$", rating_str):
                        # Juste un rating sans reviews count (ex: "5.0")
                        avg_rating = rating_str
                
                # Vérifier si Guest Favorite via badges
                is_guest_favorite_from_search = False
                badges = result.get("badges", [])
                for badge in badges:
                    logging_ctx = badge.get("loggingContext", {})
                    if logging_ctx.get("badgeType") == "GUEST_FAVORITE":
                        is_guest_favorite_from_search = True
                        break
                
                page_listings.append({
                    "room_id": str(room_id),
                    "name": result.get("title", "") or result.get("subtitle", ""),
                    "room_type": "",  # Sera enrichi par get_details
                    "person_capacity": "",
                    "bedrooms": "",
                    "beds": "",
                    "bathrooms": "",
                    "price": price,
                    "avg_rating": avg_rating,
                    "reviews_count": reviews_count,
                    "is_guest_favorite_search": is_guest_favorite_from_search,
                })
            
            all_listings.extend(page_listings)
            
            print(f"   📄 Page {page_count}: +{len(page_listings)} listings (total: {len(all_listings)})", flush=True)
            
            if not page_listings:
                break
            
            # Récupérer le cursor pour la page suivante
            pagination_info = results.get("paginationInfo", {})
            cursor = pagination_info.get("nextPageCursor")
            
            if not cursor:
                break
            
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
        return None


def extract_ratings_and_badges(details):
    """
    Extrait les notes détaillées et badges depuis les détails du listing.
    Structure confirmée par debug:
    - details["rating"] = {accuracy, checking, cleanliness, communication, location, value, guest_satisfaction, review_count}
    - details["host"] = {id, name}
    - details["is_super_host"] = True/False
    - details["is_guest_favorite"] = True/False
    - details["highlights"] = [{title, subtitle, icon}, ...]
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
        "is_superhost": False,
        "is_guest_favorite": False,
        "top_percent": "",
        "badges": "",
    }
    
    if not details:
        return result
    
    # ===== RATINGS =====
    rating_data = details.get("rating")
    if rating_data and isinstance(rating_data, dict):
        # Convertir en string pour le CSV, gérer les valeurs numériques
        accuracy = rating_data.get("accuracy")
        if accuracy is not None:
            result["rating_accuracy"] = str(accuracy)
        
        cleanliness = rating_data.get("cleanliness")
        if cleanliness is not None:
            result["rating_cleanliness"] = str(cleanliness)
        
        checking = rating_data.get("checking")  # Note: "checking" pas "checkin"
        if checking is not None:
            result["rating_checkin"] = str(checking)
        
        communication = rating_data.get("communication")
        if communication is not None:
            result["rating_communication"] = str(communication)
        
        location = rating_data.get("location")
        if location is not None:
            result["rating_location"] = str(location)
        
        value = rating_data.get("value")
        if value is not None:
            result["rating_value"] = str(value)
        
        guest_satisfaction = rating_data.get("guest_satisfaction")
        if guest_satisfaction is not None:
            result["rating_overall"] = str(guest_satisfaction)
        
        review_count = rating_data.get("review_count")
        if review_count is not None:
            result["reviews_count"] = str(review_count)
    
    # ===== HOST =====
    host_data = details.get("host")
    if host_data and isinstance(host_data, dict):
        host_id = host_data.get("id")
        if host_id is not None:
            result["host_id"] = str(host_id)
        
        host_name = host_data.get("name")
        if host_name is not None:
            result["host_name"] = str(host_name)
    
    # ===== SUPERHOST =====
    # Clé confirmée: "is_super_host" (avec underscore)
    is_superhost = details.get("is_super_host")
    if is_superhost is not None:
        result["is_superhost"] = bool(is_superhost)
    
    # ===== GUEST FAVORITE =====
    is_guest_favorite = details.get("is_guest_favorite")
    if is_guest_favorite is not None:
        result["is_guest_favorite"] = bool(is_guest_favorite)
    
    # ===== TOP X% et autres badges =====
    highlights = details.get("highlights", [])
    badges_list = []
    
    if isinstance(highlights, list):
        for highlight in highlights:
            if isinstance(highlight, dict):
                title = highlight.get("title", "")
                subtitle = highlight.get("subtitle", "")
                
                # Chercher Top X%
                combined = f"{title} {subtitle}".lower()
                if "top 1%" in combined:
                    result["top_percent"] = "1"
                elif "top 5%" in combined:
                    result["top_percent"] = "5"
                elif "top 10%" in combined:
                    result["top_percent"] = "10"
                
                # Ajouter comme badge si pertinent (mais pas tous les highlights)
                if title and any(keyword in title.lower() for keyword in ["superhost", "top", "favorite", "loved"]):
                    badges_list.append(title)
    
    # Construire la liste des badges
    final_badges = []
    if result["is_superhost"]:
        final_badges.append("Superhost")
    if result["is_guest_favorite"]:
        final_badges.append("Guest Favorite")
    if result["top_percent"]:
        final_badges.append(f"Top {result['top_percent']}%")
    
    # Ajouter les highlights uniques qui ne sont pas déjà présents
    for badge in badges_list:
        # Éviter les doublons (ex: "Myriam - Kaori Stays is a Superhost" si déjà "Superhost")
        if not any(existing.lower() in badge.lower() or badge.lower() in existing.lower() for existing in final_badges):
            final_badges.append(badge)
    
    result["badges"] = " | ".join(final_badges)
    
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
                "title": listing.get("name", "") or listing.get("title", ""),
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
    if GUEST_FAVORITE == "true":
        filters["guest_favorite"] = True
    if LUXE == "true":
        filters["luxe"] = True
    if QUERY:
        filters["query"] = QUERY
    
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
    print(f"   Coup de cœur voyageurs: {'✅ Oui' if GUEST_FAVORITE == 'true' else '(non)'}")
    print(f"   Luxe: {'✅ Oui' if LUXE == 'true' else '(non)'}")
    print(f"   Query: {QUERY or '(coordonnées)'}")
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
    
    success_count = 0
    error_count = 0
    
    for idx, listing in enumerate(listings, start=1):
        room_id = listing["room_id"]
        print(f"   [{idx}/{len(listings)}] Room {room_id}...", end=" ", flush=True)
        
        try:
            details = get_listing_details(room_id)
            
            if details:
                ratings = extract_ratings_and_badges(details)
                listing.update(ratings)
                success_count += 1
                
                # Afficher un résumé
                rating = listing.get("rating_overall", "N/A")
                badges = listing.get("badges", "")
                print(f"✓ Rating: {rating} | {badges or 'Aucun badge'}", flush=True)
            else:
                error_count += 1
                print("⚠️ Pas de détails", flush=True)
                
        except Exception as e:
            error_count += 1
            print(f"❌ Erreur: {str(e)[:50]}", flush=True)
        
        time.sleep(DELAY_BETWEEN_DETAILS)
    
    print(f"\n   ✅ Succès: {success_count} | ❌ Erreurs: {error_count}")
    
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
    top_1 = sum(1 for l in listings if str(l.get("top_percent", "")) == "1")
    top_5 = sum(1 for l in listings if str(l.get("top_percent", "")) == "5")
    top_10 = sum(1 for l in listings if str(l.get("top_percent", "")) == "10")
    
    # Calculer la moyenne des ratings
    ratings = []
    for l in listings:
        r = l.get("rating_overall")
        if r and r != "":
            try:
                ratings.append(float(r))
            except:
                pass
    
    avg_rating = sum(ratings) / len(ratings) if ratings else 0
    
    print(f"\n📊 STATISTIQUES:")
    print(f"   Total listings: {len(listings)}")
    print(f"   Avec détails: {success_count}")
    print(f"   Note moyenne: {avg_rating:.2f}" if avg_rating > 0 else "   Note moyenne: N/A")
    print(f"   Superhosts: {superhosts} ({100*superhosts/len(listings):.1f}%)" if listings else "")
    print(f"   Guest Favorites: {guest_favorites} ({100*guest_favorites/len(listings):.1f}%)" if listings else "")
    print(f"   Top 1%: {top_1}")
    print(f"   Top 5%: {top_5}")
    print(f"   Top 10%: {top_10}")
    
    print(f"\n📁 Fichier: {filename}")
    print("=" * 80)


if __name__ == "__main__":
    main()
