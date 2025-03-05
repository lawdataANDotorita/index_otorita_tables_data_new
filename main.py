import requests
import json
from supabase import create_client, Client
from openai import OpenAI
import os
import sys

def get_exe_directory():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

def get_embedding(text):
    response = openai_client.embeddings.create(
        input=text,
        model="text-embedding-3-large",
        dimensions=1536,
    )
    return response.data[0].embedding

def fetch_table_data():
    # Initialize Supabase client
    url: str = "https://rmigfbegvrilgentysif.supabase.co"
    key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJtaWdmYmVndnJpbGdlbnR5c2lmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mjk0MzEwMjMsImV4cCI6MjA0NTAwNzAyM30.S3HRecwWknLROuORA_nfOlizw5VFOeHp01ku3Y8f89M"
    supabase: Client = create_client(url, key)

    # Initialize OpenAI client
    current_dir = get_exe_directory()
    open_ai_key_path = os.path.join(current_dir, 'open_ai_key.txt')
    with open(open_ai_key_path, 'r') as f:
        open_ai_key = f.read().strip()
    global openai_client
    openai_client = OpenAI(api_key=open_ai_key)

    url = "https://otorita.net/otorita_test/maagar/tables/gettbldata.asp"
    
    try:
        # Send GET request to the URL
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse JSON response
        json_response = response.json()
        
        # Access the data array from the response
        data = json_response.get('data', [])
        
        # Template for the text line
        template = "שער ה-*1* בתאריך ה-*2* הוא *3* ש\"ח."
        
        # Create a list to store all formatted lines
        formatted_lines = []
        
        # Prepare records for Supabase
        records_to_insert = []
        
        # Create records with embeddings
        for item in data:
            formatted_text = template.replace("*1*", item["nm"]).replace("*2*", item["dt"]).replace("*3*", item["mddVl"])
            formatted_lines.append(formatted_text)
            
            try:
                embedding = get_embedding(formatted_text)
                record = {
                    "content": formatted_text,
                    "name_in_db": f"table_data_{item['dt']}",
                    "embedding": embedding,
                    "type": "table",
                }
                records_to_insert.append(record)
            except Exception as e:
                print(f"Error creating embedding for record: {e}")
        
        # Write all lines to a file
        with open('formatted_data.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(formatted_lines))
            
        print("Data has been written to 'formatted_data.txt'")
        
        # Insert records into Supabase
        if records_to_insert:
            try:
                # Delete existing records with the same name_in_db pattern
                delete_response = supabase.table('documents_for_work_world_for_lawyers').delete().like('name_in_db', 'table_data_%').execute()
                if hasattr(delete_response, 'error') and delete_response.error:
                    print(f"Error deleting existing records: {delete_response.error}")
                else:
                    print("Deleted existing table data records")
                
                # Insert new records
                response = supabase.table('documents_for_work_world_for_lawyers').insert(records_to_insert).execute()
                if hasattr(response, 'error') and response.error:
                    print(f"Error inserting records: {response.error}")
                else:
                    print(f"Successfully inserted {len(response.data)} records into Supabase")
            except Exception as e:
                print(f"Error during Supabase operations: {e}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")

if __name__ == "__main__":
    fetch_table_data() 