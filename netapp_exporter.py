import os
import logging
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, Response
from prometheus_client import Gauge, Counter, generate_latest
from datetime import datetime

# Désactivation des avertissements SSL (à utiliser avec prudence)
requests.packages.urllib3.disable_warnings()

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)

# Métriques pour les volumes
volume_used = Gauge("netapp_volume_used_bytes", "Espace utilisé sur un volume", ["volume"])
volume_size = Gauge("netapp_volume_size_bytes", "Taille totale du volume", ["volume"])
volume_available = Gauge("netapp_volume_available_bytes", "Espace disponible sur un volume", ["volume"])
volume_usage_percent = Gauge("netapp_volume_usage_percent", "Pourcentage utilisé d'un volume", ["volume"])
# Mètre les informations de métadonnées du volume (info metric : toujours à 1)
volume_info = Gauge("netapp_volume_info", "Information sur le volume (1=present)", 
                      ["volume", "style", "type", "snapshot_policy", "svm"])
# Heure de création du volume en timestamp Unix
volume_create_time = Gauge("netapp_volume_create_time", "Heure de création du volume en timestamp Unix", ["volume"])

# Métriques pour les agrégats (tiers)
tier_used = Gauge("netapp_tier_used_bytes", "Espace utilisé sur un agrégat", ["tier"])
tier_size = Gauge("netapp_tier_size_bytes", "Taille totale de l'agrégat", ["tier"])
tier_available = Gauge("netapp_tier_available_bytes", "Espace disponible sur un agrégat", ["tier"])
tier_usage_percent = Gauge("netapp_tier_usage_percent", "Pourcentage utilisé d'un agrégat", ["tier"])
tier_full_threshold_percent = Gauge("netapp_tier_full_threshold_percent", "Seuil d'alerte plein d'un agrégat", ["tier"])
tier_physical_used = Gauge("netapp_tier_physical_used_bytes", "Espace utilisé physiquement sur un agrégat", ["tier"])

# Métriques pour les nœuds (nodes)
node_uptime = Gauge("netapp_node_uptime_seconds", "Uptime du nœud en secondes", ["node"])
node_state = Gauge("netapp_node_state", "État du nœud (1 = up, 0 = down)", ["node"])
node_cpu_count = Gauge("netapp_node_cpu_count", "Nombre de CPU dans le nœud", ["node"])
node_memory_size = Gauge("netapp_node_memory_size_bytes", "Taille de la mémoire du nœud en octets", ["node"])

# Compteur pour les requêtes échouées
failed_requests = Counter("netapp_failed_requests", "Nombre de requêtes NetApp échouées", ["endpoint"])

def fetch_json(url, username, password):
    """Effectue une requête GET et renvoie le JSON en cas de succès."""
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), verify=False, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Erreur {response.status_code} lors de la récupération de {url}")
            failed_requests.labels(endpoint=url).inc()
    except Exception as e:
        logging.error(f"Exception lors de la récupération de {url} : {e}")
        failed_requests.labels(endpoint=url).inc()
    return None

def collect_volume_metrics(ontap_ip, username, password):
    # Récupère les métriques pour tous les volumes en appelant leur endpoint détaillé.
    logging.info("Récupération des volumes détaillés...")
    volumes_list_url = f"https://{ontap_ip}/api/storage/volumes"
    data = fetch_json(volumes_list_url, username, password)
    if data is None:
        logging.error("Impossible de récupérer la liste des volumes")
        return

    volumes = data.get("records", data)
    for vol in volumes:
        vol_name = vol.get("name", "Inconnu")
        self_href = vol.get("_links", {}).get("self", {}).get("href")
        if not self_href:
            logging.error(f"Aucun lien de détail pour le volume {vol_name}")
            continue
        vol_detail_url = f"https://{ontap_ip}{self_href}"
        vol_detail = fetch_json(vol_detail_url, username, password)
        if vol_detail is None:
            logging.error(f"Impossible de récupérer les détails pour le volume {vol_name}")
            continue

        # Si la section "space" existe, on récupère les métriques de stockage
        space = vol_detail.get("space", {})
        if space:
            used = space.get("used", 0)
            size = space.get("size", 0)
            available = space.get("available", 0)
            percent_used = (used / size * 100) if size > 0 else 0
            volume_used.labels(volume=vol_name).set(used)
            volume_size.labels(volume=vol_name).set(size)
            volume_available.labels(volume=vol_name).set(available)
            volume_usage_percent.labels(volume=vol_name).set(percent_used)
            logging.info(f"Volume {vol_name} (space): used={used}, size={size}, available={available}, usage={percent_used:.2f}%")
        else:
            logging.info(f"Volume {vol_name} n'a pas de métriques 'space'")

        # Récupération de la date de création et conversion en timestamp Unix
        create_time_str = vol_detail.get("create_time")
        ts = 0
        if create_time_str:
            try:
                dt = datetime.fromisoformat(create_time_str.replace("Z", "+00:00"))
                ts = dt.timestamp()
            except Exception as e:
                logging.error(f"Erreur lors du parsing de la date pour le volume {vol_name}: {create_time_str}")
        volume_create_time.labels(volume=vol_name).set(ts)

        # Collecte des informations statiques en tant que métrique "info"
        style = vol_detail.get("style", "unknown")
        vtype = vol_detail.get("type", "unknown")
        snapshot_policy = vol_detail.get("snapshot_policy", {}).get("name", "unknown")
        svm_name = vol_detail.get("svm", {}).get("name", "unknown")
        volume_info.labels(volume=vol_name, style=style, type=vtype, snapshot_policy=snapshot_policy, svm=svm_name).set(1)

def collect_tier_metrics(ontap_ip, username, password):
    """Récupère les métriques pour tous les agrégats (tiers) via leur endpoint détaillé."""
    logging.info("Récupération des agrégats détaillés...")
    aggregates_list_url = f"https://{ontap_ip}/api/storage/aggregates"
    data = fetch_json(aggregates_list_url, username, password)
    if data is None:
        logging.error("Impossible de récupérer la liste des agrégats")
        return

    aggregates = data.get("records", data)
    for agg in aggregates:
        tier_name = agg.get("name", "Inconnu")
        self_href = agg.get("_links", {}).get("self", {}).get("href")
        if not self_href:
            logging.error(f"Aucun lien de détail pour l'agrégat {tier_name}")
            continue
        agg_detail_url = f"https://{ontap_ip}{self_href}"
        agg_detail = fetch_json(agg_detail_url, username, password)
        if agg_detail is None:
            logging.error(f"Impossible de récupérer les détails pour l'agrégat {tier_name}")
            continue

        block_storage = agg_detail.get("space", {}).get("block_storage", {})
        used = block_storage.get("used", 0)
        size = block_storage.get("size", 0)
        available = block_storage.get("available", 0)
        percent_used = (used / size * 100) if size > 0 else 0

        tier_used.labels(tier=tier_name).set(used)
        tier_size.labels(tier=tier_name).set(size)
        tier_available.labels(tier=tier_name).set(available)
        tier_usage_percent.labels(tier=tier_name).set(percent_used)

        # Nouveaux champs
        full_threshold = block_storage.get("full_threshold_percent", 0)
        physical_used = block_storage.get("physical_used", 0)
        tier_full_threshold_percent.labels(tier=tier_name).set(full_threshold)
        tier_physical_used.labels(tier=tier_name).set(physical_used)

        logging.info(f"Agrégat {tier_name}: used={used}, size={size}, available={available}, usage={percent_used:.2f}%, threshold={full_threshold}%, physical_used={physical_used}")

def collect_node_metrics(ontap_ip, username, password):
    """Récupère les métriques pour tous les nœuds du cluster."""
    logging.info("Récupération des nœuds du cluster...")
    nodes_url = f"https://{ontap_ip}/api/cluster/nodes"
    data = fetch_json(nodes_url, username, password)
    if data is None:
        logging.error("Impossible de récupérer la liste des nœuds")
        return

    nodes = data.get("records", data)
    for node in nodes:
        node_name = node.get("name", "Inconnu")
        uptime = node.get("uptime", 0)
        state_str = node.get("state", "").lower()
        state_val = 1 if state_str == "up" else 0
        cpu_count = node.get("controller", {}).get("cpu", {}).get("count", 0)
        memory_size = node.get("controller", {}).get("memory_size", 0)

        node_uptime.labels(node=node_name).set(uptime)
        node_state.labels(node=node_name).set(state_val)
        node_cpu_count.labels(node=node_name).set(cpu_count)
        node_memory_size.labels(node=node_name).set(memory_size)

        logging.info(f"Nœud {node_name}: uptime={uptime}, state={state_str}, cpu_count={cpu_count}, memory_size={memory_size}")

def collect_metrics(ontap_ip, username, password):
    """Collecte toutes les métriques en parallèle pour volumes, agrégats et nœuds."""
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(collect_volume_metrics, ontap_ip, username, password)
        executor.submit(collect_tier_metrics, ontap_ip, username, password)
        executor.submit(collect_node_metrics, ontap_ip, username, password)

@app.route("/metrics")
def metrics():
    """
    Endpoint /metrics qui :
      - Lit les paramètres de connexion depuis la query string (ou via les variables d'environnement par défaut)
      - Lance la collecte des métriques NetApp
      - Retourne les métriques au format Prometheus
    """
    ontap_ip = request.args.get("ONTAP_IP") or os.getenv("ONTAP_IP", "default_ip")
    username = request.args.get("ONTAP_USER") or os.getenv("ONTAP_USER", "admin")
    password = request.args.get("ONTAP_PASS") or os.getenv("ONTAP_PASS", "password")

    logging.info(f"Collecte des métriques pour ONTAP_IP={ontap_ip}")
    collect_metrics(ontap_ip, username, password)

    output = generate_latest()
    return Response(output, mimetype="text/plain; version=0.0.4; charset=utf-8")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9110)
