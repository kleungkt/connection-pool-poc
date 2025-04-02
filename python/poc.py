import pymongo
import requests
from requests.auth import HTTPDigestAuth
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


# Configuration constants; client must provide environment details, including MongoDB & Atlas info
GROUP_ID = "xxxxxx"
CLUSTER_NAME = "xxxxxx"
API_PUBLIC_KEY = "xxxxxx"
API_PRIVATE_KEY = "xxxxxx"
PROCESS_ID = "xxxxxx"
MONGO_URI = "xxxxxx"


# API URLs
MEASUREMENTS_URL = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{GROUP_ID}/processes/{PROCESS_ID}/measurements"
CONFIG_URL = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{GROUP_ID}/clusters/{CLUSTER_NAME}"


# Cluster tier and max connections mapping
TIER_MAP = {
    "M10": 1500, "M20": 3000, "M30": 3000, "M40": 6000, "M50": 16000,
    "M60": 32000, "M80": 96000, "M140": 96000, "M200": 128000, "M300": 128000
}


# Instance memory mapping (in bytes)
MEMORY_MAP = {
    "M10": 2 * 1024 * 1024 * 1024, "M20": 4 * 1024 * 1024 * 1024,
    "M30": 8 * 1024 * 1024 * 1024, "M40": 16 * 1024 * 1024 * 1024,
    "M50": 32 * 1024 * 1024 * 1024, "M60": 64 * 1024 * 1024 * 1024
}


# Global variables
mongo_client = None
current_tier = "M10"
max_connections = TIER_MAP["M10"]
monitoring_active = True


def get_max_connections_from_atlas():
    global current_tier, max_connections
    try:
        response = requests.get(CONFIG_URL, auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY), headers={"Accept": "application/json"})
        if response.status_code != 200:
            raise Exception(f"Failed to fetch cluster config. Status: {response.status_code}, Response: {response.text}")
        config = response.json()
        current_tier = config["providerSettings"]["instanceSizeName"]
        max_connections = TIER_MAP.get(current_tier, 1500)
        print(f"[{datetime.now().isoformat()}] Retrieved cluster tier: {current_tier}, Max connections: {max_connections}")
        return max_connections
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to get max connections, using default 1500: {e}")
        return 1500


def configure_auto_scaling():
    url = CONFIG_URL
    try:
        current_config_response = requests.get(url, auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY), headers={"Accept": "application/json"})
        if current_config_response.status_code != 200:
            raise Exception(f"Failed to fetch current cluster config. Status: {current_config_response.status_code}, Response: {current_config_response.text}")
        current_config = current_config_response.json()
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to fetch current cluster config: {e}")
        current_config = {"providerSettings": {"providerName": "AWS", "instanceSizeName": "M10", "regionName": "US_EAST_1"}}


    current_provider_settings = current_config.get("providerSettings", {})
    payload = {
        "autoScaling": {
            "compute": {
                "enabled": True
            },
            "diskGBEnabled": True
        },
        "providerSettings": {
            "providerName": current_provider_settings.get("providerName", "AWS"),
            "instanceSizeName": current_provider_settings.get("instanceSizeName", "M10"),
            "regionName": current_provider_settings.get("regionName", "US_EAST_1"),
            "autoScaling": {
                "compute": {
                    "maxInstanceSize": "M60",
                    "minInstanceSize": "M10"
                }
            }
        }
    }
    try:
        response = requests.patch(url, auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY), json=payload, headers={"Content-Type": "application/json"})
        if response.status_code == 200:
            print(f"[{datetime.now().isoformat()}] Auto-scaling enabled, range: M10 to M60")
        else:
            print(f"[{datetime.now().isoformat()}] Auto-scaling config failed: Status {response.status_code}, Response {response.text}")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error configuring auto-scaling: {e}")


def get_process_measurements():
    query = {"granularity": "PT1M", "period": "PT10M", "m": ["PROCESS_CPU_USER", "PROCESS_CPU_KERNEL", "MEMORY_RESIDENT"]}
    try:
        response = requests.get(MEASUREMENTS_URL, auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY), params=query, headers={"Accept": "application/json"})
        if response.status_code != 200:
            raise Exception(f"Failed to fetch measurements. Status: {response.status_code}, Response: {response.text}")
        measurements = response.json().get("measurements", [])
        cpu_user = next((m for m in measurements if m["name"] == "PROCESS_CPU_USER"), {"dataPoints": [{"value": 0}]})
        cpu_kernel = next((m for m in measurements if m["name"] == "PROCESS_CPU_KERNEL"), {"dataPoints": [{"value": 0}]})
        memory_resident = next((m for m in measurements if m["name"] == "MEMORY_RESIDENT"), {"dataPoints": [{"value": 0}]})
        cpu_user_avg = sum(dp["value"] for dp in cpu_user["dataPoints"] if dp["value"] is not None) / max(sum(1 for dp in cpu_user["dataPoints"] if dp["value"] is not None), 1)
        cpu_kernel_avg = sum(dp["value"] for dp in cpu_kernel["dataPoints"] if dp["value"] is not None) / max(sum(1 for dp in cpu_kernel["dataPoints"] if dp["value"] is not None), 1)
        memory_avg = sum(dp["value"] for dp in memory_resident["dataPoints"] if dp["value"] is not None) / max(sum(1 for dp in memory_resident["dataPoints"] if dp["value"] is not None), 1)
        total_cpu_usage = cpu_user_avg + cpu_kernel_avg
        memory_usage = (memory_avg / MEMORY_MAP.get(current_tier, 2 * 1024 * 1024 * 1024)) * 100
        return {"cpuUsage": total_cpu_usage, "memoryUsage": memory_usage}
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to fetch measurements: {e}")
        return {"cpuUsage": 0.0, "memoryUsage": 0.0}


def get_current_connections():
    try:
        server_status = mongo_client.admin.command("serverStatus")
        current_connections = server_status["connections"]["current"]
        return current_connections
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to get current connections: {e}")
        raise


def initialize_mongo_client():
    global mongo_client, max_connections
    max_connections = get_max_connections_from_atlas()
    mongo_client = pymongo.MongoClient(
        MONGO_URI,
        maxPoolSize=max_connections,
        maxIdleTimeMS=30000,
        socketTimeoutMS=10000,
        minPoolSize=100
    )
    print(f"[{datetime.now().isoformat()}] MongoDB client initialized. Current tier: {current_tier}, Max connections: {max_connections}")


def monitor_resources():
    global monitoring_active, current_tier, max_connections
    instance_sizes = ["M10", "M20", "M30", "M40", "M50", "M60"]


    while monitoring_active:
        current_connections = get_current_connections()
        connection_usage = current_connections / max_connections
        measurements = get_process_measurements()
        cpu_usage = measurements["cpuUsage"]
        memory_usage = measurements["memoryUsage"]


        print(f"[{datetime.now().isoformat()}] Real-time monitoring - Connections: {current_connections}/{max_connections} ({connection_usage * 100:.2f}%)")
        print(f"[{datetime.now().isoformat()}] Real-time monitoring - CPU Usage: {cpu_usage:.2f}%, Memory Usage: {memory_usage:.2f}%")


        current_index = instance_sizes.index(current_tier)


        # Fetch current config as payload base
        try:
            current_config_response = requests.get(CONFIG_URL, auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY), headers={"Accept": "application/json"})
            if current_config_response.status_code != 200:
                raise Exception(f"Failed to fetch current cluster config. Status: {current_config_response.status_code}")
            current_config = current_config_response.json()
            current_provider_settings = current_config.get("providerSettings", {})
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] Failed to fetch current cluster config: {e}")
            current_provider_settings = {"providerName": "AWS", "instanceSizeName": current_tier, "regionName": "US_EAST_1"}


        # Construct base payload, consistent with configure_auto_scaling
        payload = {
            "autoScaling": {
                "compute": {
                    "enabled": True
                },
                "diskGBEnabled": True
            },
            "providerSettings": {
                "providerName": current_provider_settings.get("providerName", "AWS"),
                "instanceSizeName": current_tier,  # Default, overridden later
                "regionName": current_provider_settings.get("regionName", "US_EAST_1"),
                "autoScaling": {
                    "compute": {
                        "maxInstanceSize": "M60",
                        "minInstanceSize": "M10"
                    }
                }
            }
        }


        # Upgrade on resource overload
        if connection_usage > 0.1 or cpu_usage > 70 or memory_usage > 80:
            if current_index < len(instance_sizes) - 1:
                next_tier = instance_sizes[current_index + 1]
                print(f"[{datetime.now().isoformat()}] Resource overload, upgrading to: {next_tier}")


                payload["providerSettings"]["instanceSizeName"] = next_tier


                try:
                    response = requests.patch(
                        CONFIG_URL,
                        auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY),
                        json=payload,
                        headers={"Content-Type": "application/json"}
                    )
                    if response.status_code == 200:
                        current_tier = next_tier
                        max_connections = TIER_MAP.get(next_tier, max_connections)
                        print(f"[{datetime.now().isoformat()}] Successfully upgraded to {next_tier}, New max connections: {max_connections}")
                    else:
                        print(f"[{datetime.now().isoformat()}] Upgrade failed: Status {response.status_code}, Response {response.text}")
                except Exception as e:
                    print(f"[{datetime.now().isoformat()}] Error during upgrade: {e}")


        # Downgrade on low resource usage
        elif connection_usage < 0.4 and cpu_usage < 35 and memory_usage < 40 and current_index > 0:
            prev_tier = instance_sizes[current_index - 1]
            print(f"[{datetime.now().isoformat()}] Low resource usage, downgrading to: {prev_tier}")


            payload["providerSettings"]["instanceSizeName"] = prev_tier


            try:
                response = requests.patch(
                    CONFIG_URL,
                    auth=HTTPDigestAuth(API_PUBLIC_KEY, API_PRIVATE_KEY),
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code == 200:
                    current_tier = prev_tier
                    max_connections = TIER_MAP.get(prev_tier, max_connections)
                    print(f"[{datetime.now().isoformat()}] Successfully downgraded to {prev_tier}, New max connections: {max_connections}")
                else:
                    print(f"[{datetime.now().isoformat()}] Downgrade failed: Status {response.status_code}, Response {response.text}")
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] Error during downgrade: {e}")


        time.sleep(1)  # Check every second


def process_tenant_request(tenant_id, operation):
    try:
        db = mongo_client[f"{tenant_id}_db"]
        collection = db["data"]
        for _ in range(50):
            doc = {"operation": operation, "tenant": tenant_id, "timestamp": int(time.time() * 1000)}
            collection.insert_one(doc)
        print(f"[{datetime.now().isoformat()}] Tenant {tenant_id} executed operation: {operation} completed")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Tenant {tenant_id} operation failed: {e}")


def simulate_tenant_requests_with_monitoring(tenant_count=5000, batch_size=500):
    global monitoring_active
    monitor_thread = threading.Thread(target=monitor_resources)
    monitor_thread.start()


    total_batches = (tenant_count + batch_size - 1) // batch_size
    for batch in range(total_batches):
        start_idx = batch * batch_size
        end_idx = min(start_idx + batch_size, tenant_count)


        current_connections = get_current_connections()
        if current_connections / max_connections > 0.9:
            print(f"[{datetime.now().isoformat()}] Connection count nearing limit ({current_connections}/{max_connections}), pausing new requests...")
            time.sleep(5)
            continue


        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [
                executor.submit(process_tenant_request, f"tenant{i}", "Test request")
                for i in range(start_idx, end_idx)
            ]
            for future in futures:
                future.result()


        print(f"[{datetime.now().isoformat()}] Completed batch {batch + 1}/{total_batches}")
        time.sleep(0.1)


    monitoring_active = False
    monitor_thread.join()


if __name__ == "__main__":
    initialize_mongo_client()
    # configure_auto_scaling() can be configured as needed or adjusted via UI
    simulate_tenant_requests_with_monitoring(tenant_count=5000, batch_size=500)
    mongo_client.close()
    print(f"[{datetime.now().isoformat()}] MongoDB client closed")

