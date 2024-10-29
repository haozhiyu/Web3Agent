import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import json
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

def convert_js_to_json(js_obj):
    """将JavaScript对象转换为有效的JSON字符串"""
    # 添加引号到属性名
    js_obj = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', js_obj)
    
    # 处理undefined和单字母变量
    js_obj = re.sub(r':\s*undefined\s*([,}])', r':null\1', js_obj)
    js_obj = re.sub(r'(?<!["\w])[a-zA-Z](?!["\w])', 'null', js_obj)
    
    # 处理void 0
    js_obj = re.sub(r'void 0', 'null', js_obj)
    
    # 确保布尔值是小写的
    js_obj = js_obj.replace('True', 'true').replace('False', 'false')
    
    return js_obj

def extract_json_data(script_content):
    """提取并转换JavaScript对象为Python字典"""
    try:
        # 提取return后面的对象
        match = re.search(r'window\.__NUXT__=\(function\(.*?\){return (.*?)}\(', script_content, re.DOTALL)
        if not match:
            print("No match found in script content")
            return None
        
        js_obj = match.group(1).strip()
        # 移除最后的参数列表
        js_obj = re.sub(r',\s*\[.*?\]\)$', '', js_obj)
        
        # 转换为有效的JSON
        json_str = convert_js_to_json(js_obj)
        
        # 打印转换后的JSON字符串（用于调试）
        print("Converted JSON string:", json_str[:200] + "...")
        
        # 解析JSON
        data = json.loads(json_str)
        return data
    except Exception as e:
        print(f"Error extracting JSON: {str(e)}")
        print("Problematic JSON string:", js_obj[:200] + "...")
        return None

def scrape_blockbeats():
    url = "https://www.theblockbeats.info/newsflash"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # 寻找包含 window.__NUXT__ 的script标签
    script_content = None
    for script in soup.find_all('script'):
        if script.string and 'window.__NUXT__' in script.string:
            script_content = script.string
            break
    
    if not script_content:
        print("No script content found")
        return []
    
    # 获取一小时前的Unix时间戳
    one_hour_ago = int(time.time()) - 3600
    articles = []
    
    try:
        data = extract_json_data(script_content)
        if not data:
            return []
            
        # 遍历新闻数据
        if 'data' in data and len(data['data']) > 0:
            for day in data['data'][0].get('days', []):
                for news_item in day.get('children', []):
                    # 添加安全检查
                    add_time = news_item.get('add_time')
                    if add_time is None:
                        print(f"Warning: add_time is None for article: {news_item.get('title', 'Unknown')}")
                        continue
                    
                    try:
                        add_time = int(add_time)
                    except (ValueError, TypeError):
                        print(f"Warning: Invalid add_time format: {add_time}")
                        continue
                    
                    # 检查是否在最近一小时内
                    if add_time >= one_hour_ago:
                        title = news_item.get('title', '')
                        content = news_item.get('content', '')
                        
                        if not title or not content:
                            print(f"Warning: Missing title or content for article with add_time {add_time}")
                            continue
                        
                        # 清理HTML标签
                        try:
                            content = BeautifulSoup(content, 'html.parser').get_text()
                        except Exception as e:
                            print(f"Error cleaning HTML content: {str(e)}")
                            continue
                        
                        articles.append([title, content])
                        print(f"Found article: {title}")
    
    except Exception as e:
        print(f"Error parsing data: {str(e)}")
        # 打印更详细的错误信息
        import traceback
        print(traceback.format_exc())
    
    return articles

def save_to_s3(articles, bucket_name='haozhiyu.fun', file_key='web3knowledgebase/blockbeats_news.csv'):
    if not articles:
        print("No articles to save")
        return None
        
    # 创建临时CSV文件
    temp_file = '/tmp/blockbeats_news.csv'
    try:
        with open(temp_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Content'])
            writer.writerows(articles)
    except Exception as e:
        print(f"Error writing CSV file: {str(e)}")
        return None
    
    # 上传到S3
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(temp_file, bucket_name, file_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{file_key}")
        return f"s3://{bucket_name}/{file_key}"
    except NoCredentialsError:
        print("AWS credentials not available")
        return None
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return None

def update_knowledge_base(s3_path, kb_id):
    bedrock = boto3.client('bedrock-agent')
    
    # 生成带时间戳的唯一数据源名称
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    data_source_name = f'blockbeats_news_{timestamp}'
    
    # 构建符合要求的配置
    data_source_config = {
        "s3Configuration": {
            "bucketArn": "arn:aws:s3:::haozhiyu.fun",
            "inclusionPrefixes": ["web3knowledgebase/blockbeats_news.csv"]
        },
        "type": "S3"
    }
    
    try:
        # 创建新的数据源
        print(f"Creating new data source with name: {data_source_name}")
        response = bedrock.create_data_source(
            knowledgeBaseId=kb_id,
            name=data_source_name,  # 使用带时间戳的名称
            description=f"News data from BlockBeats created at {timestamp}",
            dataSourceConfiguration=data_source_config,
            dataDeletionPolicy="DELETE",
            vectorIngestionConfiguration={
                "chunkingConfiguration": {
                    "chunkingStrategy": "FIXED_SIZE",
                    "fixedSizeChunkingConfiguration": {
                        "maxTokens": 100,
                        "overlapPercentage": 10
                    }
                }
            }
        )
        
        data_source_id = response.get('dataSourceId')
        print(f"Created new data source with ID: {data_source_id}")
        
        # 等待数据源创建完成
        print("Waiting for data source to become available...")
        time.sleep(30)
        
        # 启动数据摄入
        print("Starting ingestion job...")
        ingestion_job = bedrock.start_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=data_source_id
        )
        
        print(f"Started ingestion job: {ingestion_job.get('ingestionJobId')}")
        
    except Exception as e:
        print(f"Error updating knowledge base: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        
def main():
    kb_id = 'GRQWIANFCM'
    
    while True:
        try:
            print("Starting to scrape BlockBeats...")
            articles = scrape_blockbeats()
            
            if articles:
                print(f"Found {len(articles)} new articles")
                s3_path = save_to_s3(articles)
                if s3_path:
                    update_knowledge_base(s3_path, kb_id)
            else:
                print("No new articles found")
            
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
        
        print("Waiting for next cycle...")
        time.sleep(3600)

if __name__ == "__main__":
    main()
