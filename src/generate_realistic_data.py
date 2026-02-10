"""
Enhanced E-commerce Synthetic Data Generator
Uses NVIDIA Nemo Data Designer API for realistic data generation
"""

import os
import sys
import pandas as pd
import json
from pathlib import Path

from client import get_data_designer_client, NEMOTRON_30B_MODEL
from schemas import DATASET_CONFIG

def generate_realistic_dataset(client, dataset_name, config):
    """Generate realistic dataset using NVIDIA Nemo Data Designer API"""
    
    schema = config["schema"]
    record_count = config["record_count"]
    
    print(f"Generating {record_count} realistic records for {dataset_name} dataset...")
    
    try:
        # Create a structured prompt for the LLM
        if dataset_name == "customers":
            prompt = f"""Generate {record_count} realistic customer records for an e-commerce store in CSV format.

Include these exact columns: customer_id,first_name,last_name,email,phone,address,registration_date,customer_tier

Requirements:
- customer_id: sequential numbers starting from 1001
- first_name: realistic American first names
- last_name: realistic surnames  
- email: format firstname.lastname@domain.com (use gmail, yahoo, outlook)
- phone: format (XXX) XXX-XXXX with valid US area codes
- address: full street address with city, state, ZIP code
- registration_date: dates between 2023-01-01 and 2024-12-31
- customer_tier: Bronze, Silver, Gold, or Platinum

Return only CSV data with headers, no extra text."""

        elif dataset_name == "products":
            prompt = f"""Generate {record_count} realistic product records for an e-commerce store in CSV format.

Include these exact columns: product_id,product_name,category,price,description,stock_quantity,brand,rating

Requirements:
- product_id: sequential numbers starting from 2001
- product_name: realistic product names (electronics, clothing, home goods)
- category: Electronics, Clothing, Home & Garden, Books, Sports, or Beauty
- price: between $5.99 and $999.99 in format $XX.XX
- description: brief product description (20-40 words)
- stock_quantity: number between 0 and 500
- brand: mix of real brands (Apple, Nike, etc) and fictional ones
- rating: between 1.0 and 5.0 with one decimal place

Return only CSV data with headers, no extra text."""

        else:  # orders
            prompt = f"""Generate {record_count} realistic order records for an e-commerce store in CSV format.

Include these exact columns: order_id,customer_id,product_id,order_date,quantity,total_amount,status,shipping_address

Requirements:
- order_id: sequential numbers starting from 3001
- customer_id: random numbers between 1001 and 1020
- product_id: random numbers between 2001 and 2020
- order_date: dates between 2024-01-01 and 2024-12-31
- quantity: between 1 and 5 items
- total_amount: between $5.99 and $2999.99 in format $XX.XX
- status: Pending, Processing, Shipped, Delivered, or Cancelled
- shipping_address: full street address with city, state, ZIP

Return only CSV data with headers, no extra text."""
        
        # Make API call to generate data
        response = client.completions(
            model=NEMOTRON_30B_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.7
        )
        
        # Parse the response to extract CSV data
        if response and hasattr(response, 'choices') and len(response.choices) > 0:
            csv_content = response.choices[0].message.content.strip()
            
            # Parse CSV content into list of dictionaries
            lines = csv_content.split('\n')
            if len(lines) < 2:
                raise ValueError("Invalid CSV response format")
            
            headers = [h.strip() for h in lines[0].split(',')]
            data = []
            
            for line in lines[1:]:
                if line.strip():
                    values = [v.strip() for v in line.split(',')]
                    if len(values) == len(headers):
                        data.append(dict(zip(headers, values)))
            
            return data
        else:
            raise ValueError("No valid response from API")
            
    except Exception as e:
        print(f"API call failed for {dataset_name}: {e}")
        print("Falling back to sample data generation...")
        return create_enhanced_sample_data(schema, record_count, dataset_name)

def create_enhanced_sample_data(schema, record_count, dataset_name):
    """Create enhanced sample data with more realistic values"""
    
    sample_data = []
    
    # Sample data pools for more realistic generation
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Jessica", 
                   "William", "Ashley", "James", "Amanda", "Christopher", "Jennifer", "Daniel"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez"]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
    cities = ["New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ"]
    
    for i in range(record_count):
        row = {}
        
        if dataset_name == "customers":
            fname = first_names[i % len(first_names)]
            lname = last_names[i % len(last_names)]
            row = {
                "customer_id": 1001 + i,
                "first_name": fname,
                "last_name": lname,
                "email": f"{fname.lower()}.{lname.lower()}@{domains[i % len(domains)]}",
                "phone": f"({555 + (i % 400):03d}) {100 + (i % 800):03d}-{1000 + (i % 9000):04d}",
                "address": f"{100 + i} Main St, {cities[i % len(cities)]} {10001 + i}",
                "registration_date": f"2024-{1 + (i % 12):02d}-{1 + (i % 28):02d}",
                "customer_tier": schema["fields"]["customer_tier"]["options"][i % 4]
            }
        elif dataset_name == "products":
            categories = schema["fields"]["category"]["options"]
            row = {
                "product_id": 2001 + i,
                "product_name": f"Premium Product {i+1}",
                "category": categories[i % len(categories)],
                "price": f"${19.99 + (i * 15.50):.2f}",
                "description": f"High-quality product with advanced features and excellent performance",
                "stock_quantity": 50 + (i * 10) % 450,
                "brand": f"Brand{chr(65 + i % 26)}",
                "rating": f"{3.0 + (i % 21) * 0.1:.1f}"
            }
        else:  # orders
            row = {
                "order_id": 3001 + i,
                "customer_id": 1001 + (i % 20),
                "product_id": 2001 + (i % 20),
                "order_date": f"2024-{1 + (i % 12):02d}-{1 + (i % 28):02d}",
                "quantity": 1 + (i % 5),
                "total_amount": f"${25.99 + (i * 12.75):.2f}",
                "status": schema["fields"]["status"]["options"][i % 5],
                "shipping_address": f"{200 + i} Oak Ave, {cities[i % len(cities)]} {20001 + i}"
            }
        
        sample_data.append(row)
    
    return sample_data

def save_to_adls_spark(data, dataset_name, abfss_base_path, spark_session):
    """Save generated data directly to ADLS using Spark DataFrame operations with abfss:// protocol"""
    from datetime import datetime
    
    if not data:
        print(f"No data to save for {dataset_name}")
        return False

    # Create date-partitioned path with abfss:// protocol (directory path, not filename)
    now = datetime.now()
    date_path = f"{dataset_name}/{now.year:04d}/{now.month:02d}/{now.day:02d}"
    timestamp = now.strftime("%Y%m%d_%H%M")
    # Spark writes to directory, not specific filename
    output_path = f"{abfss_base_path}{date_path}/{dataset_name}_{timestamp}"

    try:
        print(f"🔄 Writing {dataset_name} to {output_path}...")
        
        # Create Spark DataFrame from data
        df_spark = spark_session.createDataFrame(pd.DataFrame(data))
        
        print(f"📤 Starting Spark write to ADLS Gen2...")
        # Write as single JSON file using coalesce(1) to avoid partitioning
        df_spark.coalesce(1).write.mode("overwrite").json(output_path)
        
        print(f"✅ Wrote {len(data)} records to {output_path}")
        
        # Show sample of generated data
        print(f"📋 Sample data preview for {dataset_name}:")
        sample_df = pd.DataFrame(data)
        print(sample_df.head(3).to_string(index=False))
        print()
        
        return True
    except Exception as e:
        print(f"❌ Error writing {dataset_name}: {e}")
        print(f"   ABFSS path: {output_path}")
        print(f"   Error type: {type(e).__name__}")
        return False

def save_to_json(data, filename, output_dir):
    """Save generated data to CSV file"""
    
    if not data:
        print(f"No data to save for {filename}")
        return False
        
    output_path = Path(output_dir) / filename
    
    try:
        df = pd.DataFrame(data)
        df.to_json(output_path, orient='records', indent=2)
        print(f"✅ Saved {len(data)} records to {output_path}")
        
        # Show sample of generated data
        print(f"📋 Sample data preview for {filename}:")
        print(df.head(3).to_string(index=False))
        print()
        
        return True
    except Exception as e:
        print(f"❌ Error saving {filename}: {e}")
        return False

def main(spark_session=None, abfss_base_path=None):
    """Main function to generate all e-commerce datasets"""
    
    print("🚀 Enhanced E-commerce Synthetic Data Generation")
    print("Using NVIDIA Nemo Data Designer with Nemotron 30B")
    print("=" * 60)
    
    # Initialize client
    try:
        client = get_data_designer_client()
        print("✅ NVIDIA Nemo Data Designer client initialized")
        print(f"🤖 Using model: {NEMOTRON_30B_MODEL}")
    except Exception as e:
        print(f"❌ Failed to initialize client: {e}")
        return False
    
    # Determine output mode
    use_adls = spark_session is not None and abfss_base_path is not None
    if use_adls:
        print(f"📤 Spark ADLS mode: Writing to {abfss_base_path}")
    else:
        print("📁 Local file mode: Creating data/ directory")
        output_dir = Path(__file__).parent.parent / "data"
        output_dir.mkdir(exist_ok=True)
    
    # Generate each dataset
    results = {}
    total_records = 0
    
    for dataset_name, config in DATASET_CONFIG.items():
        print(f"\n📊 Processing {dataset_name} dataset...")
        print(f"   Target: {config['record_count']} records")
        
        data = generate_realistic_dataset(client, dataset_name, config)
        
        if data:
            # Save data to ADLS using Spark or local file based on mode
            if use_adls:
                success = save_to_adls_spark(data, dataset_name, abfss_base_path, spark_session)
            else:
                success = save_to_json(data, config["filename"], output_dir)
            results[dataset_name] = success
            if success:
                total_records += len(data)
        else:
            results[dataset_name] = False
    
    # Summary
    print("=" * 60)
    print("📈 Final Generation Summary:")
    for dataset_name, success in results.items():
        status = "✅ Success" if success else "❌ Failed" 
        filename = DATASET_CONFIG[dataset_name]["filename"]
        record_count = DATASET_CONFIG[dataset_name]["record_count"]
        print(f"  {dataset_name:12}: {status} -> {filename} ({record_count} records)")
    
    successful_datasets = sum(results.values())
    print(f"\n🎯 Results: {successful_datasets}/{len(results)} datasets generated successfully")
    print(f"📊 Total records: {total_records}")
    
    if use_adls:
        print(f"📤 Output destination: ADLS container '{container_name}'")
    else:
        print(f"📁 Output directory: {output_dir}")
    
    if successful_datasets == len(results):
        print("\n🎉 All datasets generated successfully!")
        if use_adls:
            print("✅ Data uploaded to Azure Data Lake Storage with date partitioning")
        else:
            print("Next steps:")
            print("  • Review the JSON files in the data/ directory")
            print("  • Upload to Azure Data Lake Storage")
    
    return successful_datasets == len(results)

if __name__ == "__main__":
    main()