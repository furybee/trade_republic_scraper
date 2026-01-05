import sys

sys.path.insert(0, "./lib")

import os
import json
import asyncio
import configparser
import websockets
import requests
import pandas as pd


def headers_to_dict(response):
    """
    Transforme les en-têtes de réponse HTTP en dictionnaire structuré.

    :param response: Objet de réponse HTTP.
    :return: Dictionnaire contenant les en-têtes structurés.
    """
    extracted_headers = {}
    for header, header_value in response.headers.items():
        parsed_dict = {}
        entries = header_value.split(", ")
        for entry in entries:
            key_value = entry.split(";")[0]
            if "=" in key_value:
                key, value = key_value.split("=", 1)
                parsed_dict[key.strip()] = value.strip()
        extracted_headers[header] = parsed_dict if parsed_dict else header_value
    return extracted_headers


def flatten_and_clean_json(all_data, sep="."):
    """
    Aplatit des données JSON imbriquées et préserve l'ordre des colonnes.

    :param all_data: Liste de dictionnaires JSON à aplatir.
    :param sep: Séparateur utilisé pour les clés aplaties.
    :return: Liste de dictionnaires aplatis et nettoyés.
    """
    all_keys = []  # Utilisé pour conserver l'ordre des colonnes
    flattened_data = []

    def flatten(nested_json, parent_key=""):
        """Aplatit récursivement un JSON imbriqué."""
        flat_dict = {}
        for key, value in nested_json.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                flat_dict.update(flatten(value, new_key))
            else:
                flat_dict[new_key] = value

            if new_key not in all_keys:
                all_keys.append(new_key)

        return flat_dict

    # Aplatir toutes les entrées et collecter toutes les colonnes possibles
    for item in all_data:
        flat_item = flatten(item)
        flattened_data.append(flat_item)

    # Assurer que chaque dictionnaire a toutes les colonnes, avec ordre inchangé
    complete_data = [
        {key: item.get(key, None) for key in all_keys} for item in flattened_data
    ]

    return complete_data


def transform_data_types(df):
    """
    Transforme les types de données d'un DataFrame Pandas :
    - Convertit les colonnes de type timestamp en format date français.
    - Formate les montants en valeurs numériques avec séparateur français.

    :param df: DataFrame contenant les données.
    :return: DataFrame transformé.
    """
    timestamp_columns = ["timestamp"]  # Colonnes de type timestamp
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d/%m/%Y")

    amount_columns = [
        "amount.value",
        "amount.fractionDigits",
        "subAmount.value",
        "subAmount.fractionDigits",
    ]
    for col in amount_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(
                lambda x: str(x).replace(".", ",") if pd.notna(x) else x
            )

    return df


async def connect_to_websocket():
    """
    Fonction asynchrone pour établir une connexion WebSocket à l'API de TradeRepublic.

    :return: L'objet WebSocket connecté à l'API de TradeRepublic.
    """
    websocket = await websockets.connect("wss://api.traderepublic.com")
    locale_config = {
        "locale": "fr",
        "platformId": "webtrading",
        "platformVersion": "safari - 18.3.0",
        "clientId": "app.traderepublic.com",
        "clientVersion": "3.151.3",
    }
    await websocket.send(f"connect 31 {json.dumps(locale_config)}")
    await websocket.recv()  # Réponse de connexion

    print("✅ Connexion à la WebSocket réussie!\n⏳ Veuillez patienter...")
    return websocket


async def fetch_transaction_details(websocket, transaction_id, token, message_id, save_raw=False, output_folder="out"):
    """
    Récupère les détails d'une transaction spécifique via WebSocket.

    Cette fonction envoie une requête WebSocket pour récupérer les informations détaillées d'une transaction
    spécifique en utilisant son `transaction_id`. Elle récupère ensuite une réponse et extrait les informations
    demandées de TOUTES les sections. Si une erreur ou un délai se produit, un message
    d'avertissement est imprimé. La fonction retourne un dictionnaire contenant les informations extraites de la transaction.

    :param websocket: L'objet WebSocket déjà connecté à l'API de TradeRepublic.
    :param transaction_id: L'identifiant unique de la transaction pour laquelle les détails doivent être récupérés.
    :param token: Le token de session utilisé pour l'authentification.
    :param message_id: L'identifiant du message qui est incrémenté à chaque requête pour éviter les conflits dans les abonnements.
    :param save_raw: Si True, sauvegarde la réponse brute JSON pour debug.
    :param output_folder: Dossier de sortie pour les fichiers de debug.

    :return: Un tuple contenant deux éléments :
        - `transaction_data`: Un dictionnaire avec les informations extraites de la transaction.
        - `message_id`: L'ID du message incrémenté après chaque requête pour gérer l'abonnement/désabonnement.
    """
    payload = {"type": "timelineDetailV2", "id": transaction_id, "token": token}
    message_id += 1
    await websocket.send(f"sub {message_id} {json.dumps(payload)}")
    response = await websocket.recv()
    await websocket.send(f"unsub {message_id}")
    await websocket.recv()

    start_index = response.find("{")
    end_index = response.rfind("}")
    response_data = json.loads(
        response[start_index : end_index + 1]
        if start_index != -1 and end_index != -1
        else "{}"
    )

    # Sauvegarder la réponse brute pour debug si demandé
    if save_raw:
        raw_folder = os.path.join(output_folder, "raw")
        os.makedirs(raw_folder, exist_ok=True)
        raw_path = os.path.join(raw_folder, f"{transaction_id}.json")
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(response_data, f, indent=2, ensure_ascii=False)

    transaction_data = {}

    # Extraire les données de TOUTES les sections
    for section in response_data.get("sections", []):
        section_title = section.get("title", "Unknown")
        section_data = section.get("data", [])

        # Gérer le cas où data est un dict (section header) au lieu d'une liste
        if isinstance(section_data, dict):
            # Extraire l'ISIN depuis le header
            if "icon" in section_data:
                icon_path = section_data.get("icon", "")
                isin = extract_isin_from_icon(icon_path)
                if isin:
                    transaction_data["header_isin"] = isin
            if "timestamp" in section_data:
                transaction_data["header_timestamp"] = section_data.get("timestamp")
            if "status" in section_data:
                transaction_data["header_status"] = section_data.get("status")
            continue

        for item in section_data:
            # Ignorer si item n'est pas un dictionnaire
            if not isinstance(item, dict):
                continue

            header = item.get("title")
            detail = item.get("detail") or {}

            # Gérer le cas où detail est une string ou None
            if isinstance(detail, str):
                detail = {"text": detail}
            elif not isinstance(detail, dict):
                detail = {}

            if not header:
                continue

            # Créer une clé unique avec le nom de section pour éviter les conflits
            key = f"{section_title}_{header}" if section_title not in ["Transaction", "Synthèse"] else header

            # Extraire le texte d'affichage
            text_value = detail.get("text")
            if text_value:
                transaction_data[key] = text_value

            # Extraire aussi les valeurs numériques structurées si présentes
            if "value" in detail:
                transaction_data[f"{key}_value"] = detail.get("value")
            if "currency" in detail:
                transaction_data[f"{key}_currency"] = detail.get("currency")

            # Gérer les détails imbriqués (detail.detail)
            nested_detail = detail.get("detail")
            if isinstance(nested_detail, dict):
                if "text" in nested_detail:
                    transaction_data[f"{key}_detail"] = nested_detail.get("text")
                if "value" in nested_detail:
                    transaction_data[f"{key}_detail_value"] = nested_detail.get("value")

            # IMPORTANT: Extraire les données imbriquées de Transaction
            # (quantité, prix unitaire, total) depuis detail.action.payload.sections
            action = detail.get("action") or {}
            if isinstance(action, dict) and action.get("type") == "infoPage":
                payload = action.get("payload", {})
                if isinstance(payload, dict):
                    nested_sections = payload.get("sections", [])
                    for nested_section in nested_sections:
                        nested_data = nested_section.get("data", [])
                        if isinstance(nested_data, list):
                            for nested_item in nested_data:
                                if not isinstance(nested_item, dict):
                                    continue
                                nested_title = nested_item.get("title")
                                nested_detail = nested_item.get("detail", {})
                                if isinstance(nested_detail, dict) and nested_title:
                                    nested_text = nested_detail.get("text")
                                    if nested_text:
                                        # Mapper les champs vers des noms standardisés
                                        if nested_title == "Actions":
                                            transaction_data["quantity"] = nested_text
                                        elif nested_title in ["Prix du titre", "Cours du titre"]:
                                            transaction_data["unitPrice"] = nested_text
                                        elif nested_title == "Total":
                                            transaction_data["subtotal"] = nested_text
                                        else:
                                            transaction_data[f"nested_{nested_title}"] = nested_text

    return transaction_data, message_id


def extract_isin_from_icon(icon_path):
    """
    Extrait l'ISIN depuis le chemin de l'icône Trade Republic.
    Exemple: "logos/US67066G1040/v2" -> "US67066G1040"

    :param icon_path: Le chemin de l'icône (ex: "logos/US67066G1040/v2") ou un dict avec une clé "icon"
    :return: L'ISIN extrait ou None si non trouvable
    """
    if not icon_path:
        return None

    # Gérer le cas où icon_path est un dict
    if isinstance(icon_path, dict):
        icon_path = icon_path.get("icon") or icon_path.get("asset") or icon_path.get("path")
        if not icon_path:
            return None

    # S'assurer que c'est bien une string
    if not isinstance(icon_path, str):
        return None

    parts = icon_path.split("/")
    if len(parts) >= 2:
        potential_isin = parts[1]
        # Vérifier que ça ressemble à un ISIN (2 lettres + 10 caractères alphanumériques)
        # ou un identifiant crypto (XF + code)
        if len(potential_isin) >= 10 and potential_isin[:2].isalpha():
            return potential_isin
    return None


def determine_transaction_type(subtitle, amount_value):
    """
    Détermine le type de transaction (BUY/SELL/DIVIDEND/OTHER).

    :param subtitle: Le sous-titre de la transaction (ex: "Ordre d'achat", "Ordre de vente")
    :param amount_value: La valeur du montant (négatif = achat, positif = vente)
    :return: Le type de transaction
    """
    if not subtitle:
        return "OTHER"

    subtitle_lower = subtitle.lower()
    if "achat" in subtitle_lower or "buy" in subtitle_lower:
        return "BUY"
    elif "vente" in subtitle_lower or "sell" in subtitle_lower:
        return "SELL"
    elif "dividende" in subtitle_lower or "dividend" in subtitle_lower:
        return "DIVIDEND"
    elif "intérêt" in subtitle_lower or "interest" in subtitle_lower:
        return "INTEREST"

    # Fallback basé sur le montant
    if amount_value:
        try:
            # Gérer le format français (virgule comme séparateur décimal)
            amount = float(str(amount_value).replace(",", "."))
            return "BUY" if amount < 0 else "SELL"
        except:
            pass

    return "OTHER"


async def fetch_all_transactions(token, extract_details, save_raw=False):
    """
    Fonction principale qui récupère toutes les transactions via WebSocket et les sauvegarde dans un fichier.

    Cette fonction se connecte à l'API WebSocket de TradeRepublic pour récupérer les informations
    relatives aux transactions de l'utilisateur, soit sous forme de JSON, soit sous forme de CSV.
    Si l'option `details` est activée, elle récupère les détails des transactions supplémentaires.

    Le processus implique l'abonnement à un flux de transactions, la gestion de la pagination,
    la collecte des données et leur sauvegarde dans un fichier à la fin.

    :param token: Token de session pour l'authentification. Il est nécessaire pour valider les requêtes de l'API.
    :param extract_details: Booléen déterminant si des détails supplémentaires sur chaque transaction doivent être récupérés.
                    Si `True`, chaque transaction sera enrichie de données supplémentaires ; sinon, seules les transactions de base seront récupérées.
    :param save_raw: Booléen pour sauvegarder les réponses brutes JSON pour debug.
    :return: Elle sauvegarde les données récupérées dans un fichier (soit JSON, soit CSV) dans le dossier spécifié.
    """
    all_data = []
    message_id = 0

    async with await connect_to_websocket() as websocket:
        after_cursor = None
        while True:
            payload = {"type": "timelineTransactions", "token": token}
            if after_cursor:
                payload["after"] = after_cursor

            message_id += 1
            await websocket.send(f"sub {message_id} {json.dumps(payload)}")
            response = await websocket.recv()
            await websocket.send(f"unsub {message_id}")
            await websocket.recv()
            start_index = response.find("{")
            end_index = response.rfind("}")
            response = (
                response[start_index : end_index + 1]
                if start_index != -1 and end_index != -1
                else "{}"
            )
            data = json.loads(response)

            if not data.get("items"):
                break

            for transaction in data["items"]:
                # Extraire l'ISIN depuis le path de l'icône
                icon_path = transaction.get("icon") or transaction.get("avatar", {}).get("asset")
                isin = extract_isin_from_icon(icon_path)
                if isin:
                    transaction["isin"] = isin

                # Déterminer le type de transaction
                subtitle = transaction.get("subtitle")
                amount_value = transaction.get("amount", {}).get("value")
                transaction["transactionType"] = determine_transaction_type(subtitle, amount_value)

                if extract_details:
                    transaction_id = transaction.get("id")
                    if transaction_id:
                        details, message_id = await fetch_transaction_details(
                            websocket, transaction_id, token, message_id,
                            save_raw=save_raw, output_folder=output_folder
                        )
                        transaction.update(details)

                all_data.append(transaction)

            after_cursor = data.get("cursors", {}).get("after")
            if not after_cursor:
                break

    if output_format.lower() == "json":
        output_path = os.path.join(output_folder, "trade_republic_transactions.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=4, ensure_ascii=False)
        print("✅ Données sauvegardées dans 'trade_republic_transactions.json'")
    else:
        flattened_data = flatten_and_clean_json(all_data)
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            df = df.dropna(axis=1, how="all")
            df = transform_data_types(df)
            output_path = os.path.join(output_folder, "trade_republic_transactions.csv")
            df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
            print("✅ Données sauvegardées dans 'trade_republic_transactions.csv'")


async def profile_cash(token):
    """
    Récupère les informations de profil de l'utilisateur via WebSocket.

    :param token: Le token de session utilisé pour l'authentification.
    :return: Un dictionnaire contenant les informations du profil utilisateur.
    """
    async with await connect_to_websocket() as websocket:
        payload = {"type": "availableCash", "token": token}
        await websocket.send(f"sub 1 {json.dumps(payload)}")
        response = await websocket.recv()

        start_index = response.find("[")
        end_index = response.rfind("]")
        response_data = json.loads(
            response[start_index : end_index + 1]
            if start_index != -1 and end_index != -1
            else "[]"
        )

        if output_format.lower() == "json":
            output_path = os.path.join(
                output_folder, "trade_republic_profile_cash.json"
            )
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(response_data, f, indent=4, ensure_ascii=False)
            print("✅ Données sauvegardées dans 'trade_republic_profile_cash.json'")
        else:
            flattened_data = flatten_and_clean_json(response_data)
            if flattened_data:
                df = pd.DataFrame(flattened_data)
                output_path = os.path.join(
                    output_folder, "trade_republic_profile_cash.csv"
                )
                df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
                print("✅ Données sauvegardées dans 'trade_republic_profile_cash.csv'")


def load_token(token_file=".token"):
    """
    Charge le token depuis un fichier s'il existe.

    :param token_file: Chemin du fichier token
    :return: Le token ou None si non trouvé
    """
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            token = f.read().strip()
            if token:
                return token
    return None


def save_token(token, token_file=".token"):
    """
    Sauvegarde le token dans un fichier.

    :param token: Le token à sauvegarder
    :param token_file: Chemin du fichier token
    """
    with open(token_file, "w") as f:
        f.write(token)
    print(f"✅ Token sauvegardé dans '{token_file}'")


async def test_token_validity(token):
    """
    Teste si un token est encore valide en faisant une requête simple.

    :param token: Le token à tester
    :return: True si valide, False sinon
    """
    try:
        async with await connect_to_websocket() as websocket:
            # Essayer de récupérer le cash disponible comme test
            payload = {"type": "availableCash", "token": token}
            await websocket.send(f"sub 1 {json.dumps(payload)}")
            response = await asyncio.wait_for(websocket.recv(), timeout=5)
            await websocket.send("unsub 1")

            # Si on reçoit une erreur, le token est invalide
            if "error" in response.lower() or "unauthorized" in response.lower():
                return False
            return True
    except Exception as e:
        print(f"⚠️ Erreur lors du test du token: {e}")
        return False


def authenticate(phone_number, pin, headers):
    """
    Effectue l'authentification complète avec 2FA.

    :param phone_number: Numéro de téléphone
    :param pin: Code PIN
    :param headers: Headers HTTP
    :return: Le token de session ou None en cas d'échec
    """
    response = requests.post(
        "https://api.traderepublic.com/api/v1/auth/web/login",
        json={"phoneNumber": phone_number, "pin": pin},
        headers=headers
    ).json()

    process_id = response.get("processId")
    countdown = response.get("countdownInSeconds")
    if not process_id:
        print("❌ Échec de l'initialisation de la connexion. Vérifiez votre numéro de téléphone et PIN.")
        return None

    code = input(f"❓ Entrez le code 2FA reçu ({countdown} secondes restantes) ou tapez 'SMS': ")

    if code == "SMS":
        requests.post(
            f"https://api.traderepublic.com/api/v1/auth/web/login/{process_id}/resend",
            headers=headers
        )
        code = input("❓ Entrez le code 2FA reçu par SMS: ")

    response = requests.post(
        f"https://api.traderepublic.com/api/v1/auth/web/login/{process_id}/{code}",
        headers=headers
    )
    if response.status_code != 200:
        print("❌ Échec de la vérification de l'appareil. Vérifiez le code et réessayez.")
        return None

    print("✅ Appareil vérifié avec succès!")

    response_headers = headers_to_dict(response)
    session_token = response_headers.get("Set-Cookie", {}).get("tr_session")
    if not session_token:
        print("❌ Token de connexion introuvable.")
        return None

    print("✅ Token de connexion trouvé!")
    return session_token


if __name__ == "__main__":
    # Chargement de la configuration
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Variables de configuration
    phone_number = config.get("secret", "phone_number")
    pin = config.get("secret", "pin")
    output_format = config.get(
        "general", "output_format"
    )  # Format de sortie : json ou csv
    output_folder = config.get("general", "output_folder")
    extract_details = config.getboolean("general", "extract_details", fallback=False)
    save_raw = config.getboolean("general", "save_raw", fallback=False)
    token_file = config.get("general", "token_file", fallback=".token")
    os.makedirs(output_folder, exist_ok=True)

    # Validation du format de sortie
    if output_format.lower() not in ["json", "csv"]:
        print(
            f"❌ Le format '{output_format}' est inconnu. Veuillez saisir 'json' ou 'csv'."
        )
        exit()

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }

    # Essayer de charger un token existant
    session_token = load_token(token_file)

    if session_token:
        print("🔑 Token existant trouvé, vérification de sa validité...")
        if asyncio.run(test_token_validity(session_token)):
            print("✅ Token valide, utilisation du token existant!")
        else:
            print("⚠️ Token expiré ou invalide, nouvelle authentification requise...")
            session_token = None

    # Si pas de token valide, authentification normale
    if not session_token:
        session_token = authenticate(phone_number, pin, headers)
        if not session_token:
            exit()
        # Sauvegarder le nouveau token
        save_token(session_token, token_file)

    # Exécution de la récupération des transactions
    asyncio.run(fetch_all_transactions(session_token, extract_details, save_raw))
    # Exécution de la récupération des informations de profil
    asyncio.run(profile_cash(session_token))