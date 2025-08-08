from web3 import Web3


@asset(group_name="ethereum")
def ethereum_transactions():
    # w3 = Web3(Web3.HTTPProvider(""))
    # latest_block = w3.eth.block_number
    # block = w3.eth.get_block(latest_block, full_transactions=True)
    # return block["transactions"]