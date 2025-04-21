import requests
import json
from supabase import create_client, Client
from openai import OpenAI
import os
import sys
import time
from datetime import datetime


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

    url = "https://otorita.net/otorita_test/maagar/tables/gettbldata.asp?index=9"
    
    try:
        # Send GET request to the URL
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse JSON response
        json_response = response.json()
        
        # Access the data array from the response
        data = json_response.get('data', [])
        
        # Create a list to store all formatted lines
        formatted_lines = []
        
        # Prepare records for Supabase
        records_to_insert = []
        
        # Create records with embeddings
        for item in data:
            formatted_text = item["txt"]
            formatted_lines.append(formatted_text)
            
            try:
                embedding = get_embedding(formatted_text)
                embedding_list = embedding.tolist()  # Convert to regular Python list
            
                # Convert the date from dd/MM/yyyy to yyyy-MM-dd format
                date_str = item.get("dt", "")
                if date_str:
                    try:
                        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                        formatted_date = date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        print(f"Warning: Invalid date format for date: {date_str}")
                        formatted_date = None
                else:
                    formatted_date = None

                record = {
                    "content": formatted_text,
                    "name_in_db": "table_"+item["tblName"],
                    "embedding": embedding_list,
                    "type": "table",
                    "dt": formatted_date
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
                # Process records one by one to identify problematic ones
                for index, record in enumerate(records_to_insert):
                    try:
                        print(f"Inserting record {index + 1} of {len(records_to_insert)}")
                        response = supabase.table('documents_for_work_world_for_lawyers').insert(record).execute()
                        print(f"Successfully inserted record {index + 1}")
                        # Add a small delay between records to prevent overwhelming the connection
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"Error inserting record {index + 1}: {str(e)}")
                        print(f"Record content: {record['content'][:100]}...")  # Print first 100 chars of content
                        continue
            except Exception as e:
                print(f"Error during Supabase operations: {str(e)}")
                print("Full error details:", e.__class__.__name__)
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")

if __name__ == "__main__":
    fetch_table_data() 