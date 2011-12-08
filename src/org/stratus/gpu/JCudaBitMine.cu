#include<openssl/sha.h>
#include <stdio.h>
#include <string.h>



typedef struct block_header {
        unsigned char shainput[76];
        unsigned long nonce;
} block_header;

void byte_swap(unsigned char* data, int len) {
        int c;
        unsigned char tmp[len];

        c=0;
        while(c<len)
        {
                tmp[c] = data[len-(c+1)];
                c++;
        }

        c=0;
        while(c<len)
        {
                data[c] = tmp[c];
                c++;
        }
}

extern "C"

__global__ void inversehash(int n,char Input[],long Nonce[], char Target[],long Output)
{
    	int i = blockIdx.x * blockDim.x + threadIdx.x;
        if (i<n)
        {
        blockheader header;
        memcpy (header.shainput,Input,76);
        header.nonce = Nonce[i];
        unsigned char hash1[SHA256_DIGEST_LENGTH];
	unsigned char hash2[SHA256_DIGEST_LENGTH];
	SHA256_CTX sha256_pass1, sha256_pass2;
    	SHA256_Init(&sha256_pass1);
    	SHA256_Update(&sha256_pass1, (unsigned char*)header,76+sizeof(long));
    	SHA256_Final(hash1, &sha256_pass1);
    	SHA256_Init(&sha256_pass2);
    	SHA256_Update(&sha256_pass2,hash1,SHA256_DIGEST_LENGTH);
    	SHA256_Final(hash2, &sha256_pass2);
    	byte_swap(hash2, SHA256_DIGEST_LENGTH);
        if(strcmp (hash2,Target) == 0)
         {
              Output = i;
         }
        }

}
