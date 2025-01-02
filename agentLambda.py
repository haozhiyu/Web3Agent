import json
import boto3
import requests
from web3 import Web3
from math import log10
from datetime import datetime, timedelta 

# 连接到AMB以太坊节点

aws_region = "us-east-1"

#AMB accessor token
accessor_token = ""

#Your Address 发送方账户的公钥
from_address = ""

vitalikaddr = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"


#Your private key name stored in Secret manager
secret_name = "eth_pri_key"

#CoinGecko private key for making calls
coingecko_pri_key = ""

endpoint_https = f"https://nd-parusya6uzdqzdcp7fzatqyjsu.t.ethereum.managedblockchain.us-east-1.amazonaws.com/?billingtoken={accessor_token}"
w3 = Web3(Web3.HTTPProvider(endpoint_https))

# Check for connection to the Ethereum network
if not w3.is_connected():
    raise ConnectionError("Failed to connect to HTTPProvider")



def lambda_handler(event, context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']

    
    def sendtx(receiver,amount):
        
        print(f"Original receiver: {receiver}")
        
        # 检查是否为 ENS 域名，如果是则解析
        resolved_address = resolve_ens(receiver)
        if resolved_address:
            receiver = resolved_address
        
        print(f"Final receiver address: {receiver}")

        sender_private_key = get_secret()

        # 定义交易参数
        transaction = {
                'from': from_address,
                'to': receiver,
                'value': w3.to_wei(amount, 'ether'),  # 发送以太币
                'gas': 21000,  # 设置 gas 限制
                'gasPrice': w3.to_wei(150, 'gwei'),
                'nonce': w3.eth.get_transaction_count(from_address),
        }
            # 签名交易
        print(transaction)
        signed_tx = w3.eth.account.sign_transaction(transaction, private_key=sender_private_key)
            
            # 发送交易
        #tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        
        try:
        # 发送交易
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        except Exception as e:
            # 处理异常
            print(f"Error sending transaction: {e}")
            return "余额不足，发送失败"
        # 你可以在这里添加其他错误处理逻辑,如记录错误、重试等
        else:
            # 交易成功发送
            print(f"Transaction sent with hash: {tx_hash.hex()}")

            # 等待交易被矿工打包
            tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
                
            print(f"Transaction successful with hash: {tx_hash.hex()}")
                
            return tx_hash.hex()
    
    
    def estimate_gas(from_address, to_address, value, data='', gas_price=None):
    
        # 检查连接
        if not w3.is_connected():
            raise Exception("Failed to connect to Ethereum network")
    
        # 准备交易数据
        transaction = {
            'from': from_address,
            'to': to_address,
            'value': w3.to_wei(value, 'ether'),  # 将ETH转换为Wei
            'data': data,
        }
    
        # 如果提供了gas价格，添加到交易中
        if gas_price:
            transaction['gasPrice'] = w3.to_wei(gas_price, 'gwei')
    
        try:
            # 估算gas
            gas_estimate = w3.eth.estimate_gas(transaction)
            return gas_estimate
        except Exception as e:
            print(f"Error estimating gas: {e}")
            return None

        
    def getBalance(address):

        # 检查是否为 ENS 域名，如果是则解析
        if address.endswith('.eth'):
            address = resolve_ens(address)
        
        balance = w3.eth.get_balance(address)

        # 将余额从 Wei 转换为 Ether
        ether_balance = w3.from_wei(balance, 'ether')
        
        print(f"Account {address} has a balance of {ether_balance} Ether")
        
        return ether_balance
        
    def getCryptoPrice(coin):
        
        url = "https://api.coingecko.com/api/v3/coins/markets"
        
        params = {
        "vs_currency": "usd",
        "ids": coin
        }
        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key":coingecko_pri_key
        }
        
        response = requests.get(url, params=params, headers=headers)
        
        print(response.text)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                price = data[0]["current_price"]
                return price
            else:
                return f"No data found for {coin}"
        else:
            return f"Error: {response.status_code} - {response.text}"
    
    #计算九神指数
    def investAdviceMetric():
        
        birth_date = datetime(year=2009, month=1, day=3)
        today = datetime.now()
        age_days = (today - birth_date).days
        
        target_price = 10 ** (5.84 * log10(age_days) - 17.01)
        
        print('target_price:', target_price)
        
        start_date = datetime.now() - timedelta(days=200) 
        # 使用CoinGecko API获取比特币的历史价格数据 
        url = f"https://api.coingecko.com/api/v3/coins/bitcoin/history?date={start_date.strftime('%d-%m-%Y')}" 
        headers = {
                    "accept": "application/json",
                    "x-cg-demo-api-key":coingecko_pri_key
                }
                
        response = requests.get(url,  headers=headers)
        data = response.json() 
        # 计算200天平均价格 
        price_sum = 0 
        for i in range(200): 
            price_sum += data["market_data"]["current_price"]["usd"] 
        average_200_day_price = price_sum / 200 
            
        cur_price_url = 'https://api.coindesk.com/v1/bpi/currentprice/USD.json'
        response = requests.get(cur_price_url, allow_redirects=True)
        cur_price = response.json()["bpi"]["USD"]["rate_float"]
        print('current price:', cur_price)
        
        p1 = cur_price / target_price
        p2 = cur_price / average_200_day_price
        p = p1 * p2
        print()
        print('all-in range: <=0.45')
        print('DCA range: 0.45 ~ 1.20')
        print('Sell range: >= 5.0')
        print()
        
        # Before a bull run, stop DCA when >=3.0
        # After a bull run, start DCA when <= 1.2
        print('ahr_999_index:', '{:.2f}'.format(p))
        
        if p <= 0.45:
            print('  Suggestion: ALL IN!')
            return 'Now the price is the lowest in history cycles, I recommend you to ALL in'
        elif p <= 1.20:
            print('  Suggestion: Dollar cost averaging.')
            return 'Now the price is relatively low in history cycles, I recommend you to keep buying regularly'

        elif p < 5.0:
            print('  Suggestion: Stop buying.')
            return 'Now the price is relatively high in history cycles, I recommend you to stop buying'

        elif p >= 5.0:
            print('  Suggestion: Sell some BTC to secure some fiat for daily spending.')
            return 'Now the price is very high in history cycles, I recommend you to sell it all now'

        
    
    #从Secret Manager获取私钥
    def get_secret():
    
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=aws_region
        )
    
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e
    
        secret_key = get_secret_value_response['SecretString']
        
        return secret_key

    #检查是否为ens域名，是则执行解析
    def resolve_ens(ens_name):
        try:
            # Check if the name is a valid ENS name
            if not ens_name.endswith('.eth'):
                return None  # Not an ENS name, return None

            # Resolve the ENS name to an Ethereum address
            address = w3.ens.address(ens_name)
            
            if address is None:
                print(f"The ENS name {ens_name} is not registered or does not have an address set.")
                return None
            else:
                print(f"The address for {ens_name} is: {address}")
                return address
        
        except Exception as e:
            print(f"An error occurred while resolving ENS: {e}")
            return None
        
        
    
    if function == "sendtx":
        parameters = {param['name']: param['value'] for param in event['parameters']}
    
        print (parameters)
        
        amount = parameters.get('amount')
        receiver = parameters.get('receiver')
        
        result = sendtx(receiver,amount)
        responseBody =  {
        "TEXT": {
            "body": result
        }
    }
    
    elif function == "estimateGas":
        value = 0.000001  # ETH
        result = estimate_gas(from_address, vitalikaddr, value)

        responseBody =  {
        "TEXT": {
            "body": result
        }
    }
    
    
    elif function =="getBalance":
        parameters = {param['name']: param['value'] for param in event['parameters']}
    
        print (parameters)
        address = parameters.get('address')
        result = getBalance(address)
        responseBody =  {
        "TEXT": {
            "body": result
        }
    }
    
    elif function =="getCryptoPrice":
        parameters = {param['name']: param['value'] for param in event['parameters']}
    
        print (parameters)
        coin = parameters.get('coin')
        result = getCryptoPrice(coin)
        responseBody =  {
        "TEXT": {
            "body": result
        }
    }
    elif function =="investAdviceMetric":

        result = investAdviceMetric()
        responseBody =  {
        "TEXT": {
            "body": result
        }
    }
        

    action_response = {
        'actionGroup': actionGroup,
        'function': function,
        'functionResponse': {
            'responseBody': responseBody
        }

    }

    function_response = {'response': action_response, 'messageVersion': event['messageVersion']}
    print("Response: {}".format(function_response))

    return function_response
