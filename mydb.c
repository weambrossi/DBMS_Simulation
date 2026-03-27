/* mydb.c
 * Ethan Ambrossi
 * CPSC375
 * MAR 25 2026
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
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
	char relname[10];
	int header[BLKMAX];
} relheader_t;

relheader_t relheaders[DISKSIZE];
int numrels = 0;  // tracks how many relations exist

int* getHeader(char *relname) {
	for (int i = 0; i < numrels; i++) {
		if (strcmp(relheaders[i].relname, relname) == 0)
			return relheaders[i].header;
	}
	return NULL;  // not found
}

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
int hash(char *tuple) {
	char key[10];
	sscanf(tuple, "%s", key);
	int sum = 0;
	for (int i = 0; i < strlen(key); i++) {
    	sum += key[i];
	}
	int	bucketnum = sum % 16;
	return bucketnum;
}
/* Disk IO operations */
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

// Purpose: initializes the specified header buffer and associates with the given header block.
// Parameters: char *relname - name of the relation
int dbcreate(char *relname) {
	int headerblknum;

	// make sure relation does not already exist
	if (strcmp(relname, "catalog") == 0)
		headerblknum = 1;
	else if (strcmp(relname, "columns") == 0)
		headerblknum = 2;
	else {
		headerblknum = findFreeBlock();
		if (headerblknum == -1) {
			printf("dbcreate error: no free disk block available\n");
			return -1;
		}
	}

	// add relation to relheaders table
	strcpy(relheaders[numrels].relname, relname);

	// initialize its in-memory header to all -1
	for (int i = 0; i < BLKMAX; i++) {
		relheaders[numrels].header[i] = -1;
	}

	// write header array to disk
	diskwrite(headerblknum, (char *) relheaders[numrels].header);

	numrels++;

	return headerblknum;
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
	int hashvalue = hash(tuple);
	int blocknum;
	int inserted = 0;

	int *header = getHeader(relname);
	if (header == NULL) {
		printf("dbput error: relation '%s' not found\n", relname);
		return;
	}

	if (header[hashvalue] == -1) {
		blocknum = findFreeBlock();
		header[hashvalue] = blocknum;

		// initialize empty block
		for (int i = 0; i < 4; i++) {
			datablk[i].flag = 0;
			datablk[i].tuple[0] = '\0';
		}
	} else {
		blocknum = header[hashvalue];
		diskread(blocknum, (char *)datablk);
	}

	// insert into first free slot
	for (int i = 0; i < 4; i++) {
		if (datablk[i].flag == 0) {
			datablk[i].flag = 1;
			strcpy(datablk[i].tuple, tuple);
			inserted = 1;
			break;
		}
	}

	if (!inserted) {
		printf("dbput error: block overflow for relation '%s'\n", relname);
		return;
	}

	diskwrite(blocknum, (char *)datablk);
}

void do_create() {
	char relname[9];
	int n, k;
	scanf("%s %d %d", relname, &n, &k);

    // read n attribute specs
    char attrnames[n][9];
    char attrdomains[n];

    for (int i = 0; i < n; i++) {
        char domain[2];
        scanf("%s %s", attrnames[i], domain);
		if (domain[0] == 'I')
			attrdomains[i] = 1;
		else
			attrdomains[i] = 0;
    }

	int relptr = dbcreate(relname);
    char cattuple[RECSIZE];
	sprintf(cattuple, "%s 0 %d %d 0 %d", relname, n, k, relptr);

    // add to catalog: "relname kind attsize keysize relsize relptr"
    sprintf(cattuple, "%s 0 %d %d 0 %d", relname, n, k, relptr);
    dbput("catalog", cattuple);

    // add to columns: one tuple per attribute
    char coltuple[RECSIZE];
    for (int i = 0; i < n; i++) {
        sprintf(coltuple, "%s %s %d %d", relname, attrnames[i], attrdomains[i], i);
        dbput("columns", coltuple);
    }

} 

void do_delete() {
    char relname[9];
    scanf("%s", relname);
    dbdelete(relname);
}

void do_insert() {
    char relname[9];
    int n;
    scanf("%s %d", relname, &n);
    char tuple[RECSIZE];
	int c;
    while ((c = getchar()) != '\n' && c != EOF);  // remove leftover newline

    for (int i = 0; i < n; i++) {
        fgets(tuple, RECSIZE, stdin);        
		tuple[strcspn(tuple, "\n")] = '\0';  // strip trailing newline
        // check key uniqueness
        if (dbget(relname, tuple) != 0) {
            printf("INSERT error: duplicate key in relation '%s'\n", relname);
            continue;
        }
        dbput(relname, tuple);
    }
}

void do_remove() {
    char relname[9];
    int n;
    scanf("%s %d", relname, &n);
    char tuple[RECSIZE];

    int c;
    while ((c = getchar()) != '\n' && c != EOF);
    for (int i = 0; i < n; i++) {
        fgets(tuple, RECSIZE, stdin);
		tuple[strcspn(tuple, "\n")] = '\0';
        if (dbget(relname, tuple) == 0) {
            printf("REMOVE error: key not found in relation '%s'\n", relname);
            continue;
        }
        dbremove(relname, slotnum);
    }
}

void do_update() {
    char relname[9];
    int n;
    scanf("%s %d", relname, &n);
    char tuple[RECSIZE];
	
	int c;
    while ((c = getchar()) != '\n' && c != EOF);

    for (int i = 0; i < n; i++) {
        fgets(tuple, RECSIZE, stdin);
		tuple[strcspn(tuple, "\n")] = '\0';
        if (dbget(relname, tuple) == 0) {
            printf("UPDATE error: key not found in relation '%s'\n", relname);
            continue;
        }
        dbupdate(relname, slotnum, tuple);
    }
}

void do_print() {
    char relname[9];
    scanf("%s", relname);
    printf("%s\n", relname);
    // print column headers from columns relation
    // then print all tuples via dbread
    char tuple[RECSIZE];
    while (dbread(relname, tuple) != 0) {
        printf("%s\n", tuple);
    }
}

void do_union() {
    char p[9], q[9], r[9];
    scanf("%s %s %s", p, q, r);

    // create new derived relation r
    dbcreate(r);

    // add all tuples from p
    char tuple[RECSIZE];
    while (dbread(p, tuple) != 0) {
        dbwrite(r, tuple);
    }

    // add tuples from q only if not already in r
    while (dbread(q, tuple) != 0) {
        if (dbget(r, tuple) == 0)
            dbwrite(r, tuple);
    }

    // update catalog and columns for r
    // (copy column info from p since r has same attributes)
}

void do_difference() {
    char p[9], q[9], r[9];
    scanf("%s %s %s", p, q, r);

    dbcreate(r);

    char tuple[RECSIZE];
    while (dbread(p, tuple) != 0) {
        if (dbget(q, tuple) == 0)
            dbwrite(r, tuple);
    }
}

void do_projection() {
    char p[9], q[9];
    int n;
    scanf("%s %s %d", p, q, &n);

    char attrnames[n][9];
    for (int i = 0; i < n; i++)
        scanf("%s", attrnames[i]);

    dbcreate(q);

    char tuple[RECSIZE];
    while (dbread(p, tuple) != 0) {
        // extract only the specified attributes
        char projected[RECSIZE];
        // project tuple fields here based on attrnames
        // then suppress duplicates
        if (dbget(q, projected) == 0)
            dbwrite(q, projected);
    }
}

void do_selection() {
    char p[9], q[9];
    int n;
    scanf("%s %s %d", p, q, &n);

    char ops[n][3];
    char vals[n][9];
    for (int i = 0; i < n; i++)
        scanf("%s %s", ops[i], vals[i]);

    dbcreate(q);

    char tuple[RECSIZE];
    while (dbread(p, tuple) != 0) {
        // check all n conditions against tuple
        // if all pass, write to q
        dbwrite(q, tuple);
    }
}

void do_naturaljoin() {
    char p[9], q[9], r[9];
    int n;
    scanf("%s %s %s %d", p, q, r, &n);

    char attrnames[n][9];
    for (int i = 0; i < n; i++)
        scanf("%s", attrnames[i]);

    dbcreate(r);

    char ptuple[RECSIZE], qtuple[RECSIZE];
    while (dbread(p, ptuple) != 0) {
        while (dbread(q, qtuple) != 0) {
            // check if common attributes match
            // if yes, combine and write to r
        }
    }
}

int main(void) {

	bitmapblk[0] = 1;  // bitmap block
	bitmapblk[1] = 1;  // catalog header
	bitmapblk[2] = 1;  // columns header
	// initialize headerblocks
	dbcreate("catalog");
	dbcreate("columns");
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
