DROP KEYSPACE IF EXISTS graphsense_raw_xmr;
CREATE KEYSPACE graphsense_raw_xmr WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE graphsense_raw_xmr;

// XMR blockchain data



-- Tables that take input from csv files
-- Should be normalized later
DROP TABLE IF exists inputs;
CREATE TABLE inputs (
    time TIMESTAMP WITHOUT TIME ZONE,
    block DECIMAL NOT NULL,
    txhash VARCHAR NOT NULL,
    key_image VARCHAR NOT NULL,
    ring_size DECIMAL NOT NULL,
    absolute_key_offset DECIMAL NOT NULL, -- origin block -1 (or +1)
    ref_output_pubk VARCHAR NOT NULL, --referenced_output_pub_key
    ref_txhash VARCHAR NOT NULL, --referenced_txhash
    ref_out_index DECIMAL NOT NULL --reference_out_index_in_the_ref_tx
);

DROP TABLE IF exists outputs;
CREATE TABLE outputs (
    time TIMESTAMP WITHOUT TIME ZONE,
    block DECIMAL NOT NULL,
    txhash VARCHAR NOT NULL,
    tx_public_key VARCHAR NOT NULL,
    tx_version INTEGER NOT NULL,
    payment_id VARCHAR NOT NULL, --???
    out_idx DECIMAL NOT NULL,
    amount DECIMAL NOT NULL,
    output_pubk VARCHAR NOT NULL,
    output_key_img VARCHAR NOT NULL, --???
    output_spend BOOLEAN NOT NULL --???
);


----! Normalized schema

---- No idea if concept of block is useful for my purposes. Therefore currently omitted
-- CREATE TABLE block (
--     height int PRIMARY KEY,
--     block_hash blob,
--     timestamp int,
--     block_version int,
--     size int,
--     txs list<blob>
-- );


CREATE TYPE input (
    keyimg text
    ring list<FROZEN<output>>
    PRIMARY KEY(keyimg)
);

CREATE TYPE output (
    pubk text, -- pubkey instead of address. this should be unique. non-unique values exist but they should be cleaned up
    value decimal, -- use bigint instead? 
    spent_in ring
    PRIMARY KEY(pubk) -- what happens if not unique?
    -- Not needed?
    -- n int, -- index of output. not sure if useful
    -- addresses list<text> -- why multiple addresses per output? isn't it 1:1?
);

CREATE TABLE transaction (
    txid TIMEUUID
    height int, -- block height
    tx_hash text, -- originally used blob. no idea which is better
    timestamp timestamp,
    coinbase boolean,
    vin list<FROZEN<input>>,
    vout list<FROZEN<output>>,
    PRIMARY KEY (block_group, txid)
    -- Not needed?
    -- block_group int, 
    -- size int,
    -- tx_number int, -- not needed
);

CREATE TABLE exchange_rates (
    timestamp int PRIMARY KEY,
    eur double,
    usd double
);

----! No tag implementation yet
-- CREATE TABLE tag (
--     address text,
--     tag text,
--     tag_uri text,
--     description text,
--     actor_category text,
--     source text,
--     source_uri text,
--     timestamp int,
--     PRIMARY KEY (address, tag, source)
-- );

