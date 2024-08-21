import sys
from collections import deque
import requests
import itertools
from time import sleep, time
from web3 import Web3
from web3.middleware import simple_cache_middleware

## Author: Mathias DAIL, Co-CEO at Exorde Labs
## Date: 2024-08-21
## mathias@exordelabs.com

## Notice: this code will only work if the IPFS gateway is UP and doesn't timeout, same for the SyncNode we read state info.
#          It is important to not spam/dos the SyncNode with too many requests, otherwise you will be IP banned.
##         This code is a simple example of how to monitor new URLs from the Exorde network in real time.
###        This code makes the example of updating a local seen_urls.txt with all unique URLs found in the Exorde network.

# //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# to print URLs
DEBUG_MODE = False

# mapping network ids
mainnet_selected = True
network_name_id = { "testnet":"testnet-B", "mainnet":"testnet-A" }
ipfs_gateway_1 = "http://ipfs-gateway.exorde.network/ipfs/"

# No need to change these values
MAX_RETRIES = 3
BATCH_SIZE = 10
offset_to_last_batch_id = 10


MAX_FILES_TO_REMEMBER = 1_000_000
MAX_URLS_TO_REMEMBER = 60_000
URLs_TO_KEEP_IN_FILE = 500_000
# if 20k files per hour, then 3 hours is ~60k files
# 1 million URL in bytes is approx 100MB because 1 URL is approx 100 bytes

####################################################################################################
# Function to load JSON data from a URL
def load_json_from_url(url):
    response = requests.get(url)
    data = response.json()
    return data

def extract_sync_nodes(data):
    return data.get("urlSkale", None)

def select_object_by_network_id(json_data, network_id):
    data = json_data
    for obj in data:
        if obj.get("_networkId") == network_id:
            return obj
    return None

# this function assumes is_mainnet_select equal to NOT is_testnet_selected
def get_network_info(is_mainnet_selected):
    # URL of NetworkConfig.json
    network_config_url = "https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ClientNetworkConfig.json"

    # Load NetworkConfig.json
    network_config_data = load_json_from_url(network_config_url)['testnet-A']

    chain_endpoint = None
    selected_network_config = None
    if is_mainnet_selected:
        selected_network_config = select_object_by_network_id(network_config_data,network_name_id["mainnet"])
    else:
        selected_network_config = select_object_by_network_id(network_config_data,network_name_id["testnet"])

    assert(selected_network_config is not None)

    chain_endpoint = selected_network_config["_urlSkale3"]
    chainID = selected_network_config["_chainID"] 
    webSocket = selected_network_config["_webSocket"] 
    syncNodes = extract_sync_nodes(selected_network_config)

    assert(chain_endpoint is not None)
    assert(chainID is not None)
    assert(webSocket is not None)
    return {"chain_endpoint":chain_endpoint, "chainID":chainID, "webSocket":webSocket, "syncNodes": syncNodes}

# this function assumes is_mainnet_select equal to NOT is_testnet_selected
def get_addresses_contracts(is_mainnet_selected):
    # URL of ContractsAddresses.json
    contracts_addresses_url = "https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ContractsAddresses.json"

    # Load ContractsAddresses.json
    contracts_addresses_data = load_json_from_url(contracts_addresses_url)

    selected_addresses = None
    # Convert the data to dictionaries
    if is_mainnet_selected:
        selected_addresses = contracts_addresses_data[network_name_id["mainnet"]]
    else:
        selected_addresses = contracts_addresses_data[network_name_id["testnet"]]

    return selected_addresses

def initialize_web3(networkConfig):
    assert len(networkConfig['syncNodes']) > 0
    assert len(networkConfig["chain_endpoint"]) > 0
    w3Tx = Web3(Web3.HTTPProvider(networkConfig["chain_endpoint"]))        
    w3 = Web3(Web3.HTTPProvider(networkConfig['syncNodes'][2])) # take 1st sync node by default
    try:
        w3.middleware_onion.add(simple_cache_middleware)
    except Exception as e:
        print("w3 middleware error: ",e)
    
    return w3, w3Tx

def get_contracts_abis_from_git():
	to = 10
	abis = dict()
	abis["EXDT"] = requests.get("https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ABIs/EXDT/EXDT.json", timeout=to).json()
	abis["DataSpotting"] = requests.get("https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ABIs/DataSpotting.sol/DataSpotting.json", timeout=to).json()
	abis["StakingManager"] = requests.get("https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ABIs/StakingManager.sol/StakingManager.json", timeout=to).json()
	abis["ConfigRegistry"] = requests.get("https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ABIs/ConfigRegistry.sol/ConfigRegistry.json", timeout=to).json()
	abis["AddressManager"] = requests.get("https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/main/ABIs/AddressManager.sol/AddressManager.json", timeout=to).json()
	abis["Parameters"] = requests.get("https://raw.githubusercontent.com/MathiasExorde/TestnetProtocol-staging/main/ABIs/Parameters.sol/Parameters.json", timeout=to).json()

	return abis

def chunked(it, size):
   it = iter(it)
   while True:
      p = tuple(itertools.islice(it, size))
      if not p:
         break
      yield p

def get_last_batch_id(data_contract):    
    LastDataBatchId = None
    for k in range(0, MAX_RETRIES):
        try:                       
            LastDataBatchId = data_contract.functions.getLastCheckedBatchId().call()
        except KeyboardInterrupt:
            print("Stop me!")
            sys.exit(0)
        except Exception as e :
            sleep(int(2*k+1)) # exponential backoff
            continue
        break
    return LastDataBatchId

print("-------------------------")
is_mainnet_selected = True
network_info = get_network_info(is_mainnet_selected)
contracts_addresses = get_addresses_contracts(is_mainnet_selected)
base_abis = get_contracts_abis_from_git()

####################################################################################################
####################################################################################################
####################################################################################################
chain_endpoint = network_info["chain_endpoint"]
print("\nNetwork Info = ",network_info)
print("Chain endpoint = ",chain_endpoint)
w3 = Web3(Web3.HTTPProvider(chain_endpoint))
dataspotting_addr = contracts_addresses['DataSpotting']
print(f"Addresses: \nDataSpotting = {dataspotting_addr}")


### ------------------- Monitor all spots in real time ------------------------------
# Initialize the contract
# Then, get the last batch ID
# Then, iterate over each batch ID and get the CID of each file
# Then, download the file from IPFS
# Then, read  URLs  from the file and store them in a dict
# Then, store the URLs in a database

def find_real_time_URLs():
    ############################################################
    # THIS IS WHERE YOU CONNECT TO A DB
    # db_cursor, db_connection = init_db_connection()

    collected_cids = deque(maxlen=MAX_FILES_TO_REMEMBER)
    urls_seen = deque(maxlen=MAX_URLS_TO_REMEMBER)
    while True:
        try:
            ############### - READ GLOBAL CONTRACT STATE
            data_contract = w3.eth.contract(dataspotting_addr, abi=base_abis['DataSpotting']["abi"])
            LastDataBatchId = get_last_batch_id(data_contract)
            print("Last Spotted Exorde Data Batch ID = ",LastDataBatchId)
            if LastDataBatchId is None or int(LastDataBatchId) == 0:
                continue
            start_idx = LastDataBatchId  -  offset_to_last_batch_id 
            if start_idx < 1: 
                start_idx = 1
            if LastDataBatchId - start_idx < 2: 
                print("\nNot enough new files...")
                sleep(1)
                continue

            ############### Read new URLs by batch
            index_range = list(range(start_idx, LastDataBatchId))
            if DEBUG_MODE:
                print("Number of chunks = ", len(index_range), " with batch size = ", BATCH_SIZE)
            urls_found_in_pass = 0
            for chunk in chunked(index_range, BATCH_SIZE):
                start_idx_ =   chunk[0]
                end_idx_ =   chunk[-1]
                if start_idx_ == end_idx_:
                    end_idx_ += 1

                sleep(1) # do not remove or you will get banned IP from the read only Sync Nodes of the Exorde network                
                files_chunk = data_contract.functions.getBatchsFilesByID(start_idx_, end_idx_).call() # do not call with huge range of indexes

                #########################################
                #    DOWNLOAD FILE FROM CHUNK ON IPFS   #
                #########################################
                for ipfs_cid in files_chunk:
                    # SKIP if already downloaded file
                    if ipfs_cid in collected_cids:
                        continue
                    collected_cids.append([ipfs_cid])
                    file_content = None
                    file_url = ipfs_gateway_1 + ipfs_cid
                    try:
                        if DEBUG_MODE:
                            print("\tDownloading file from IPFS: ",file_url)
                        file_content = requests.get(file_url, timeout=3).json()
                        # Browse the JSON from { "items" : [ { "item": "url" } ] } and iterate all item in the list
                        for item in file_content["items"]:
                            url = item["item"]['url']
                            if url not in urls_seen:
                                urls_seen.append([url])
                                urls_found_in_pass += 1
                                if DEBUG_MODE:
                                    print("\t\tNew spotted URL: ",url)

                                ### THIS IS WHERE YOU CAN STORE IN A Database like Redis or MySQL
                                ## ......
                                ##############

                                # example: we update a local seen_urls.txt file with all unique URLs
                                try:
                                    with open("seen_urls.txt","a+", encoding='utf-8') as f:
                                        f.write(url+"\n")
                                except Exception as e:
                                    print("Error writing URL to local file: ",e)
                                    continue

                    except Exception as e:
                        print("Error: ",e)
                        continue
            
            print("\n\tURLs found in this pass = ",urls_found_in_pass)
            # print stats about deques
            print("\tTotal URLs seen so far = ",len(urls_seen))
            print("\tTotal files seen so far = ",len(collected_cids))

            ## periodically remove all URLs from the seen_urls.txt file if it gets over MAX_URLS_TO_REMEMBER
            # open the local file and count lines
            # if the count is > URLs_TO_KEEP_IN_FILE, calculate surplus and remove the first surplus lines (oldest URLs)
            try:
                with open("seen_urls.txt","r", encoding='utf-8') as f:
                    lines = f.readlines()
                    if len(lines) > URLs_TO_KEEP_IN_FILE:
                        surplus = len(lines) - URLs_TO_KEEP_IN_FILE
                        with open("seen_urls.txt","w", encoding='utf-8') as f:
                            f.writelines(lines[surplus:])
            except Exception as e:
                print("Error reading URL from local file: ",e)

        except KeyboardInterrupt:
            print("Stop me!")
            sys.exit(0)
        except Exception as e:
            print("error: ",e)
        sleep(3)

################### EXAMPLE MAIN
find_real_time_URLs()
