import base64
import json
import urllib.request
import os

def fetch_diagram(mmd_file, output_png):
    with open(mmd_file, 'r', encoding='utf-8') as f:
        code = f.read()
    
    payload = json.dumps({
        "code": code,
        "mermaid": {
            "theme": "default"
        }
    })
    
    encoded_payload = base64.urlsafe_b64encode(payload.encode('utf-8')).decode('ascii')
    url = f"https://mermaid.ink/img/{encoded_payload}"
    print(f"Fetching {output_png} from {url[:50]}...")
    
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req) as response, open(output_png, 'wb') as out_file:
            out_file.write(response.read())
        print(f"Success: {output_png}")
    except Exception as e:
        print(f"Failed to fetch {output_png}: {e}")

if __name__ == "__main__":
    fetch_diagram("arsitektur.mmd", "arsitektur_medallion.png")
    fetch_diagram("erd.mmd", "erd_star_schema.png")
    fetch_diagram("lineage.mmd", "dbt_lineage_dag.png")
    
