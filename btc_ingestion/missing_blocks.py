#!/usr/bin/env python3

import time
import sys
import os
import mysql.connector
from blockchain_parser.blockchain import Blockchain
from blockchain_parser.undo import get_block_transaction_undos

code = sys.argv[1] # eg., BTC
path = sys.argv[2] # eg., /mnt/BTC/blocks

mysql_database = 'swatt'
mysql_user = 'root'
mysql_host = sys.argv[3]
mysql_password = sys.argv[4]
mysql_port = sys.argv[5]

start = sys.argv[6]
end = sys.argv[7]

block_index_cache = '.ingest_block_index.%s.pickle' % code

block_data_sql =  ( """ REPLACE INTO BLOCK_DATA ( BLOCKCHAIN_CODE, HASH, 
      TRANSACTION_COUNT, HEIGHT, DIFFICULTY, DIFFICULTY_SCALE, REWARD, 
      REWARD_SCALE, MERKLE_ROOT, TIMESTAMP, BITS, SIZE, VERSION_HEX, NONCE,
      PREV_HASH, NEXT_HASH, AVG_FEE, AVG_FEE_SCALE, AVG_FEE_RATE,
      AVG_FEE_RATE_SCALE, INDEXED, LARGEST_TX_HASH, LARGEST_TX_AMOUNT,
      LARGEST_TX_AMOUNT_SCALE, LARGEST_FEE, LARGEST_FEE_SCALE, SMALLEST_FEE,
      SMALLEST_FEE_SCALE, INDEXING_DURATION, IS_CLEAN )
      VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    """ )

def insert_block_data(connection, block_data):
    cursor = connection.cursor()
    cursor.execute(block_data_sql, block_data)
    connection.commit()
    cursor.close()

def to_block_data(blockchain, block, block_undo):
    block_reward = 0

    total_fee = 0
    total_fee_rate = 0
    largest_fee = 0
    smallest_fee = 10000000000000000
    largest_tx_amount = 0
    largest_tx_hash = None

    transaction_count = 1 # why not 0?

    txundos = get_block_transaction_undos(block_undo.hex)
    for i, tx in enumerate(block.transactions):
        txundo = next(txundos)
        amount = 0
        in_value = 0
        fee = 0
        fee_rate = 0

        for no, output in enumerate(tx.outputs):
            tx_id = '%s|%d' % (tx.txid, no)
            amount += (output.value / 10**8 )

        if not tx.is_coinbase():
            in_value = sum([txin_undo.amount for txin_undo in txundo.txin_undos])
            in_value = in_value / 10**8

            if in_value > 0:
                fee = in_value - amount
                fee_rate = (1000 * fee) / tx.size

                largest_fee = fee if fee > largest_fee else largest_fee
                smallest_fee = fee if fee < smallest_fee else smallest_fee

                transaction_count += 1
                total_fee += fee
                total_fee_rate += fee_rate

                if amount > largest_tx_amount:
                    largest_tx_amount = amount
                    largest_tx_hash = tx.hash
        else:
            block_reward += amount

    if smallest_fee == 10000000000000000:
        smallest_fee = 0
    else:
        smallest_fee = smallest_fee * (10**11)

    return ( code,
      block.hash,
      transaction_count,
      block.height,
      block.header.difficulty * (10**2),
      2,
      (block_reward - total_fee) * (10**11), #reward
      11,
      block.header.merkle_root,
      block.header.timestamp.timestamp(),
      block.header.bits,
      block.size,
      block.header.version,
      block.header.nonce,
      block.header.previous_block_hash,
      blockchain.get_block_index_by_height(block.height + 1).hash, #next_hash
      (total_fee / transaction_count) * (10**11), #avg_fee
      11,
      (total_fee_rate / transaction_count) * (10**11), #avg_fee_rate
      11,
      time.time(),
      largest_tx_hash,
      largest_tx_amount * (10**11),
      11,
      largest_fee * (10**11),
      11,
      smallest_fee,
      11,
      0,
      1 )

def main():
    print('Starting ingestion at height %d' % start)

    connection = mysql.connector.connect(host=mysql_host,
                                         user=mysql_user,
                                         database=mysql_database,
                                         port=mysql_port,
                                         password=mysql_password)
    cursor = connection.cursor()

    blockchain = Blockchain(path, cache=block_index_cache)
    for block, block_undo in blockchain.get_ordered_blocks(start=start, end=end):
        t = time.time()

        if block_undo is None:
            continue

        block_data = to_block_data(blockchain, block, block_undo)
        insert_block_data(connection, block_data)
        
        print(time.time(), time.time() - t, block.height, 'block_count')

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
