import requests, json
url = 'http://localhost:11434/api/generate'
payload = {
    'model': 'llama3.2',
    'prompt': 'Return a JSON array with 2 objects. Each object must have keys: speaker, emotion, text, action.',
    'format': 'json',
    'stream': False
}
r = requests.post(url, json=payload, timeout=60)
print(repr(r.json().get('response','')[:600]))
