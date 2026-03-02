-- Enable PL/Python extension
CREATE EXTENSION IF NOT EXISTS plpython3u;

-- Decode a uint256 from ABI-encoded hex data
-- Returns NUMERIC to handle the full 256-bit range
CREATE OR REPLACE FUNCTION decode_uint256(hex_data TEXT)
RETURNS NUMERIC
LANGUAGE plpython3u
AS $$
    if hex_data is None or hex_data in ('0x', ''):
        return None
    from eth_abi import decode
    value = decode(['uint256'], bytes.fromhex(hex_data[2:]))[0]
    return value
$$;

-- Extract an address from a 32-byte padded topic
-- e.g. 0x000000000000000000000000abcd...1234 -> 0xabcd...1234
CREATE OR REPLACE FUNCTION decode_address_topic(hex_topic TEXT)
RETURNS TEXT
LANGUAGE plpython3u
AS $$
    if hex_topic is None or len(hex_topic) < 42:
        return None
    return '0x' + hex_topic[-40:]
$$;
