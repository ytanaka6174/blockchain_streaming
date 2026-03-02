select
    decode_address_topic(split_part(topics, ',', 2)) as from_address,
    decode_address_topic(split_part(topics, ',', 3)) as to_address,
    decode_uint256(raw_data)                          as amount
from {{ source('raw', 'events') }}
