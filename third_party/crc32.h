#ifndef __BSD_CRC32_H_
#define __BSD_CRC32_H_

#include <stdint.h>

uint32_t crc32(const void *buf, size_t size);
uint32_t crc32c(uint32_t crc32c, const unsigned char *buffer, unsigned int length);
uint32_t crc32c_combine(uint32_t crc1, uint32_t crc2, unsigned int length2);

#endif
