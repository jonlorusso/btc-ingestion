# Copyright (C) 2015-2016 The bitcoin-blockchain-parser developers
#
# This file is part of bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of bitcoin-blockchain-parser, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.

import os
import struct
import stat
#import plyvel

#from .block import Block
#from .index import DBBlockIndex
#from .utils import format_hash

from .index import _read_varint
from .transaction import Transaction
from .block_header import BlockHeader
from .utils import format_hash, decode_varint, double_sha256, decode_uint64, decode_uint32, decompress_amount


def get_block_transaction_undos(raw_hex):
    """Given the raw hexadecimal representation of a blockUndo,
    yields the block's transactionUndos
    """
    # Decoding the number of transactions, offset is the size of
    # the varint (1 to 9 bytes)
    n_transactions, offset = decode_varint(raw_hex)

    for i in range(n_transactions + 1):
        transaction = TransactionUndo(raw_hex[offset:])
        yield transaction

        # Skipping to the next transaction
        offset += transaction.size

class BlockUndo(object):
    """
    Represents a Bitcoin blockUndo, contains its header and its transactionUndos.
    """

    def __init__(self, raw_hex):
        self.hex = raw_hex
        self._transactions = None
        self._n_transactions = None
        self.size = len(raw_hex)

    @property
    def n_transactions(self):
        """Return the number of transactions contained in this block,
        it is faster to use this than to use len(block.transactions)
        as there's no need to parse all transactions to get this information
        """
        if self._n_transactions is None:
            self._n_transactions = decode_varint(self.hex[80:])[0]

        return self._n_transactions

    @property
    def transactions(self):
        """Returns a list of the blockUndo's transactionUndos represented
        as TransactionUndo objects"""
        if self._transactions is None:
            self._transactions = list(get_block_transaction_undos(self.hex))

        return self._transactions

class TransactionUndo(object):
    def __init__(self, raw_hex):
        self.raw_hex = raw_hex
        self.size = 0

    @property
    def txin_undos(self):
        n_inputs, offset = decode_varint(self.raw_hex)
        for i in range(n_inputs):
            txin_undo = TransactionInUndo(self.raw_hex[offset:])
            yield txin_undo
            offset += txin_undo.size
        self.size += offset
          

class TransactionInUndo(object):
    def __init__(self, raw_hex):
        height, offset = _read_varint(raw_hex)
        self.height = height // 2

        self.version = 0
        if self.height > 0:
            self.version, read = _read_varint(raw_hex[offset:])
            offset += read

        amount_compressed, read = _read_varint(raw_hex[offset:])
        self.amount = decompress_amount(amount_compressed)
        offset += read

        script_size, varint_size = _read_varint(raw_hex[offset:])

        start = raw_hex[offset:offset+1]
        if start == b'\x00':
            offset += 21
        elif start == b'\x01':
            offset += 21
        elif start == b'\x02':
            offset += 33
        elif start == b'\x03':
            offset += 33
        elif start == b'\x04':
            offset += 33
        elif start == b'\x05':
            offset += 33
        else:
            offset += ( varint_size + script_size - 6 )
 
        self.size = offset

    def __repr__(self):
        return "TxInUndo(height=%d, version=%d, amount=%f)" % ( self.height, self.version, self.amount )
        

