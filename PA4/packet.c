#include <stdint.h> 
#include <stddef.h>
#include "packet.h"


uint32_t checksum(const unsigned char *data, size_t len){
    // CRC-32 implementation

    uint32_t generator = 0x04C11DB7;
    uint8_t rem = 0;	
    uint8_t topbit = 8 * sizeof(uint8_t) - 1;


    // long division by byte
    for (int i = 0; i < len; ++i)
    {
        // brings down the next byte in long division step
        rem ^= (data[i] << (7 * sizeof(uint8_t)));

        // long division by bit
        for (uint8_t j = 8; j > 0; --j)
        {
            // try to divide the current bit
            if (rem & topbit) {
                rem = (rem << 1) ^ generator;
            }
            else {
                rem = (rem << 1);
            }
        }
    }

    
    return (rem);
};