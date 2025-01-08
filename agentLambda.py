import json
import boto3
import requests
from web3 import Web3
from math import log10
from datetime import datetime, timedelta 


aws_region = "us-east-1"

#AMB accessor token
accessor_token = ""

#Your blockchain Address 
from_address = ""

vitalikaddr = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

#Your private key name stored in Secret manager
secret_name = ""

#CoinGecko private key for making calls
coingecko_pri_key = ""

#eth_endpoint_https = f"https://nd-parusya6uzdqzdcp7fzatqyjsu.t.ethereum.managedblockchain.us-east-1.amazonaws.com/?billingtoken={eth_accessor_token}"

#We use Polygon here
polygon_endpoint_https = f"https://mainnet.polygon.managedblockchain.us-east-1.amazonaws.com/?billingtoken={polygon_accessor_token}"

w3 = Web3(Web3.HTTPProvider(polygon_endpoint_https))

# Get chain ID
chain_id = w3.eth.chain_id
print(f"Connected to network with chain ID: {chain_id}")

# Check for connection to the network
if not w3.is_connected():
    raise ConnectionError("Failed to connect to HTTPProvider")




def lambda_handler(event, context):
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']

    
    def sendtx(receiver,amount):
        
        print(f"Original receiver: {receiver}")
        
        # Check if it's an ENS domain, if so resolve it
        resolved_address = resolve_ens(receiver)
        if resolved_address:
            receiver = resolved_address
        
        print(f"Final receiver address: {receiver}")

        sender_private_key = get_secret()
        key_data = json.loads(sender_private_key)
        sender_private_key = key_data.get("eth_private_key")

        # Define transaction parameters
        transaction = {
                'from': from_address,
                'to': receiver,
                'value': w3.to_wei(amount, 'ether'),  
                'gas': 21000,  # 
                'gasPrice': w3.to_wei(150, 'gwei'),
                'nonce': w3.eth.get_transaction_count(from_address),
                'chainId': chain_id,

        }
        
        # Sign the transaction
        print(transaction)
        signed_tx = w3.eth.account.sign_transaction(transaction, private_key=sender_private_key)
            
        # Send the transaction
        try:
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        except Exception as e:
            print(f"Error sending transaction: {e}")
            return "Transaction failed"
       
        else:
            print(f"Transaction sent with hash: {tx_hash.hex()}")

            tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
                
            print(f"Transaction successful with hash: {tx_hash.hex()}")
                
            return tx_hash.hex()
    
    
    def estimate_gas(from_address, to_address, value, data='', gas_price=None):
    

        if not w3.is_connected():
            raise Exception("Failed to connect to the network")
    
        # Prepare transaction data
        transaction = {
            'from': from_address,
            'to': to_address,
            'value': w3.to_wei(value, 'ether'),  
            'data': data,
        }
    
        # If gas price is provided, add it to the transaction
        if gas_price:
            transaction['gasPrice'] = w3.to_wei(gas_price, 'gwei')
    
        try:
            # Estimate
            gas_estimate = w3.eth.estimate_gas(transaction)
            return gas_estimate
        except Exception as e:
            print(f"Error estimating gas: {e}")
            return None

        
    def getBalance(address):

        #Check if it's an ENS domain, if so resolve it
        if address.endswith('.eth'):
            address = resolve_ens(address)
        
        balance = w3.eth.get_balance(address)

        # Convert balance from Wei to Ether
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

    def investAdviceMetric():
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=365&interval=daily"
        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": coingecko_pri_key
        }
        
        response = requests.get(url, headers=headers)
        data = response.json()
        
        prices = [price[1] for price in data['prices']]
        current_price = prices[-1]
        all_time_high = max(prices)  
        
        # Calculate 200-day moving average
        ma_200 = sum(prices[-200:]) / min(200, len(prices))
        
        ath_ratio = current_price / all_time_high
        ma_ratio = current_price / ma_200
        
        sbci = (ath_ratio + ma_ratio) / 2
            
        print(f"Current Price: ${current_price:.2f}")
        print(f"All Time High: ${all_time_high:.2f}")
        print(f"200-day Moving Average: ${ma_200:.2f}")
        print(f"Simple Bitcoin Cycle Index: {sbci:.2f}")
        print("Index ranges:")
        print("0.00 - 0.25: Extremely Undervalued")
        print("0.25 - 0.50: Undervalued")
        print("0.50 - 0.75: Fair Value")
        print("0.75 - 1.00: Overvalued")
        print("1.00+: Extremely Overvalued")
            
        if sbci <= 0.25:
            return "The market appears extremely undervalued. Consider a significant investment, but be aware of potential further downside."
        elif sbci <= 0.50:
            return "The market appears undervalued. This might be a good opportunity for dollar-cost averaging or increasing your position."
        elif sbci <= 0.75:
            return "The market seems to be around fair value. Hold your current position and continue to monitor the market."
        elif sbci <= 1.00:
            return "The market appears overvalued. Consider taking some profits or reducing your position."
        else:
            return "The market appears extremely overvalued. This might be a good time to take significant profits."
    

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

    #Check ENS address
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
