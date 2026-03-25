#include <stdint.h> 
#include <stddef.h>
#include "packet.h"


uint8_t checksum(const unsigned char *data, size_t len)
{
    uint8_t rem = 0;
    const int rem_len = 8 * sizeof(uint8_t);         
    const uint8_t msb_mask = 1 << (rem_len - 1);        
    const uint8_t generator = 0x07;                

    // processing byte by byte
    for (int byte = 0; byte < len; ++byte) {

        // simulates bringing down the next byte in long div
        rem ^= (data[byte] << (rem_len - 8));

        // processing bit by bit
        for (uint8_t bit = 8; bit > 0; --bit)
        {
            // simulating XOR
            if (rem & msb_mask) {
                rem = (rem << 1) ^ generator;
            }
            else {
                rem = (rem << 1);
            }
        }
    }

    return rem;
}