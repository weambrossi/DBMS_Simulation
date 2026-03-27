/* mydb.c
 * Ethan Ambrossi
 * CPSC375
 * MAR 25 2026
 */
#include <stdio.h>
#define RECSIZE     63      /* Size of data record in bytes */
#define BLKSIZE    256      /* Size of a disk block in bytes */
#define BLKFAC       4      /* Blocking factor */
#define DISKSIZE   256      /* Total number of disk blocks */
#define BLKMAX      16      /* Maximum number of data blocks in a relation */
#define OVBLKMAX     4      /* Maximum number of overflow blocks in a relation */

/* Buffers for data blocks or overflow blocks */
typedef struct slot {
   char flag;               /* 0 = free; 1 = allocated to a record */
   char tuple[RECSIZE];
} slot_t;

slot_t datablk[BLKFAC];
char bitmapblk[DISKSIZE];   /* Buffer for the bitmap block.
                                 0 for bitmapblk[i] means that block i is free
                                 1 means it is allocated
                            */

typedef struct {
	char relname[9]; // max length of 8 chars + null terminator
	int kind;
	int attsize;
	int keysize;
	int relsize; 
	int relptr;
} catalog;


typedef struct {
	char relname[9];
	char attname[20];
	int attdomain;
	int attposition;
} column;

/* In addition, you must declare one data buffer and header buffer for each dictionary
   relation. Declare also variables to hold addresses of disk blocks read into these
   buffers.
*/

int columnHeader[BLKMAX];
int catalogHeader[BLKMAX];

/* Depending on your implementation you might add or delete some of the arguments
   specified in the following functions */

// Function: findFreeBlock()
// Purpose: Look through bitmapblk (bitmap) to find a free block) 
// Returns: The free index of bitmap
int findFreeBlock() {
    for (int i = 0; i < 256; i++) {
        if (bitmapblk[i] == 0) {
            bitmapblk[i] = 1;  // mark it allocated immediately
            return i;
        }
    }
    return -1;  // disk full
}

// Purpose: applies the hash function on a name of a relation
// Parameter: char *name - the name of the relation
// Returns: the bucket number
int hash(char *relname) {
	int sum = 0;
	for (int i = 0; i < strlen(relname); i++) {
    	sum += relname[i]; 
	}
	int	bucketnum = sum % 16;
	return bucketnum;
}
/* Disk IO operations */

// Purpose: initializes the specified header buffer and associates with the given header block.
// Parameters: char *relname - name of the relation
void dbcreate(char *relname, char *headerblk) {
	int blocknum;
    
    if (strcmp(relname, "catalog") == 0) {

        blocknum = 1;
    } else if (strcmp(relname, "columns") == 0) {
        blocknum = 2;
    } else
        blocknum = findFreeBlock();
	}
	
	memcpy(headerblk, bocknum, BLKSIZE);
	
	diskwrite(blocknum, headerblk);
}
	


/* Read 256 bytes of data from the specified disk block into the specified buffer */
void diskread(int blocknum, char *buffer) {
	FILE *stream = fopen("disk.db", "rb");
	fseek(stream, blocknum * BLKSIZE, SEEK_SET);	
	fread(buffer, sizeof(char), 256, stream);
	fclose(stream);
}

/* computes the hash value of the tuple. If a block exists for the hash value, read the block into the data buffer. If the block is full, read an overflow block into the data buffer. If no block exists for that hash value, create a block and initialize the data buffer. Finally, it inserts the given tuple into a free slot in the data buffer and flushes the buffer. */
void dbput(char *relname, char *tuple) {

}

	


/* Write 256 bytes of data from the specified buffer into the specified disk block */
void diskwrite(int blocknum, char *buffer) {
	FILE *stream = fopen("disk.db", "r+b");
	if(stream == NULL) {
		stream = fopen("disk.db", "w+b");
	}
	fseek(stream, blocknum * BLKSIZE, SEEK_SET);
	fwrite(buffer, sizeof(char), 256, stream); 
	fclose(stream);
	}

void do_create() {
	char relname[9];
	int n, k;
	scanf("%s %d %d", relname, &n, &k);
    // Read r n k from stdin
	// Read n attribute specs (name + domain) from stdin
	// Update the catalog (add a tuple for this new relation)
	// Update the columns (add n tuples, one per attribute)
	// Actually create the relation's storage structure via dbcreate()

} 


int main(void) {

	bitmapblk[0] = 1;  // bitmap block
	bitmapblk[1] = 1;  // catalog header
	bitmapblk[2] = 1;  // columns header
	// initialize headerblocks
	dbcreate("catalog", catalogHeader);
	dbcreate("columns", columnHeader);
	//write catalog into catalog
	dbput("catalog", "catalog 0 6 1 2 1");
	dbput("catalog", "columns 0 4 2 10 2");

	//write initial columns for catalog
	dbput("columns", "catalog Relname 0 0");
	dbput("columns", "catalog Kind 1 1");
	dbput("columns", "catalog Attsize 1 2");
	dbput("columns", "catalog Keysize 1 3");
	dbput("columns", "catalog Relsize 1 4");
	dbput("columns", "catalog Relptr 1 5");

	//write initial columns for columns
	dbput("columns", "columns Relname 0 0");
	dbput("columns", "columns Attname 0 1");
	dbput("columns", "columns Attdomain 1 2");
	dbput("columns", "columns Attposition 1 3");




}
