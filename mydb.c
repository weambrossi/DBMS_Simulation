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
#define MAXATTR     16      /* Maximum number of attributes in a relation */
#define MAXRELS     64      /* Maximum number of relations in memory */
#define MAXTUPLES   80      /* Maximum number of tuples in a relation */
#define MAXBLOCKS   20      /* Maximum number of blocks in a relation */
#define RELNAMELEN  32      /* Maximum internal relation name size */
#define ATTNAMELEN  20      /* Maximum internal attribute name size */

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
	char relname[RELNAMELEN];
	int kind;
	int attsize;
	int keysize;
	int relsize;
	int relptr;
	char attrnames[MAXATTR][ATTNAMELEN];
	int attrdomains[MAXATTR];
	char tuples[MAXTUPLES][RECSIZE];
	int scanpos;
	int datablocks[MAXBLOCKS];
	int datablockcount;
} relation_t;

typedef struct {
	char relname[RELNAMELEN];
	int kind;
	int attsize;
	int keysize;
	int relsize;
	int relptr;
} catalog;

typedef struct {
	char relname[RELNAMELEN];
	char attname[20];
	int attdomain;
	int attposition;
} column;

relation_t relations[MAXRELS];
int numrels = 0;
int slotnum = -1;

/* Depending on your implementation you might add or delete some of the arguments
   specified in the following functions */

// Function: resetBlock()
// Purpose: Clears a block buffer so every slot is empty
// Parameters: slot_t *block - the block buffer to clear
void resetBlock(slot_t *block) {
	for (int i = 0; i < BLKFAC; i++) {
		block[i].flag = 0;
		block[i].tuple[0] = '\0';
	}
}

// Function: initDisk()
// Purpose: Creates a fresh simulated disk file
void initDisk() {
	FILE *stream = fopen("disk.db", "wb");
	char zeros[BLKSIZE];
	memset(zeros, 0, sizeof(zeros));

	if (stream == NULL)
		return;

	for (int i = 0; i < DISKSIZE; i++)
		fwrite(zeros, sizeof(char), BLKSIZE, stream);

	fclose(stream);
}

// Function: getRelationIndex()
// Purpose: Finds the array index of the given relation
// Parameter: char *relname - the relation to search for
// Returns: the array index or -1 if it was not found
int getRelationIndex(char *relname) {
	for (int i = 0; i < numrels; i++) {
		if (strcmp(relations[i].relname, relname) == 0)
			return i;
	}
	return -1;
}

// Function: getRelation()
// Purpose: Returns a pointer to the named relation
// Parameter: char *relname - the relation to search for
// Returns: a pointer to the relation or NULL if missing
relation_t *getRelation(char *relname) {
	int index = getRelationIndex(relname);

	if (index == -1)
		return NULL;

	return &relations[index];
}

// Function: isDictionaryRelation()
// Purpose: Checks whether the relation is one of the dictionary relations
// Parameter: char *relname - the relation name to check
// Returns: 1 if it is catalog or columns and 0 otherwise
int isDictionaryRelation(char *relname) {
	return strcmp(relname, "catalog") == 0 || strcmp(relname, "columns") == 0;
}

// Function: findFreeBlock()
// Purpose: Look through bitmapblk (bitmap) to find a free block)
// Returns: The free index of bitmap
int findFreeBlock() {
    for (int i = 0; i < DISKSIZE; i++) {
        if (bitmapblk[i] == 0) {
            bitmapblk[i] = 1;  // mark it allocated immediately
            return i;
        }
    }
    return -1;  // disk full
}

// Function: freeBlock()
// Purpose: Releases a disk block back to the bitmap
// Parameter: int blocknum - the block to free
void freeBlock(int blocknum) {
	if (blocknum >= 0 && blocknum < DISKSIZE)
		bitmapblk[blocknum] = 0;
}

// Purpose: applies the hash function on a tuple key
// Parameter: char *tuple - the tuple whose first field starts the key
// Returns: the bucket number
int hash(char *tuple) {
	char key[RECSIZE];
	int sum = 0;

	if (sscanf(tuple, "%62s", key) != 1)
		return 0;

	for (int i = 0; i < (int)strlen(key); i++)
    	sum += key[i];

	return sum % BLKMAX;
}

/* Disk IO operations */
/* Write 256 bytes of data from the specified buffer into the specified disk block */
void diskwrite(int blocknum, char *buffer) {
	FILE *stream = fopen("disk.db", "r+b");

	if(stream == NULL)
		stream = fopen("disk.db", "w+b");

	if (stream == NULL)
		return;

	fseek(stream, blocknum * BLKSIZE, SEEK_SET);
	fwrite(buffer, sizeof(char), BLKSIZE, stream);
	fclose(stream);
}

/* Read 256 bytes of data from the specified disk block into the specified buffer */
void diskread(int blocknum, char *buffer) {
	FILE *stream = fopen("disk.db", "rb");

	if (stream == NULL) {
		memset(buffer, 0, BLKSIZE);
		return;
	}

	fseek(stream, blocknum * BLKSIZE, SEEK_SET);
	fread(buffer, sizeof(char), BLKSIZE, stream);
	fclose(stream);
}

// Function: splitTuple()
// Purpose: Breaks a tuple string into its separate attribute values
// Parameters: char *tuple - the tuple to split
//             char fields[][RECSIZE] - output array for the values
// Returns: how many fields were found
int splitTuple(char *tuple, char fields[][RECSIZE]) {
	char temp[RECSIZE];
	int count = 0;
	char *token;

	strcpy(temp, tuple);
	token = strtok(temp, " ");

	while (token != NULL && count < MAXATTR) {
		strcpy(fields[count], token);
		count++;
		token = strtok(NULL, " ");
	}

	return count;
}

// Function: buildTuple()
// Purpose: Joins separate field values into one tuple string
// Parameters: char *tuple - output tuple buffer
//             char fields[][RECSIZE] - field values to join
//             int count - number of fields to join
void buildTuple(char *tuple, char fields[][RECSIZE], int count) {
	tuple[0] = '\0';

	for (int i = 0; i < count; i++) {
		if (i != 0)
			strcat(tuple, " ");
		strcat(tuple, fields[i]);
	}
}

// Function: getAttrIndex()
// Purpose: Finds the position of an attribute in a relation
// Parameters: relation_t *rel - relation to search
//             char *attrname - attribute to search for
// Returns: the attribute position or -1 if it was not found
int getAttrIndex(relation_t *rel, char *attrname) {
	for (int i = 0; i < rel->attsize; i++) {
		if (strcmp(rel->attrnames[i], attrname) == 0)
			return i;
	}
	return -1;
}

// Function: appendTuple()
// Purpose: Appends a tuple to a relation without any duplicate checks
// Parameters: relation_t *rel - relation receiving the tuple
//             char *tuple - tuple string to append
// Returns: 1 on success and 0 if the relation is full
int appendTuple(relation_t *rel, char *tuple) {
	if (rel->relsize >= MAXTUPLES)
		return 0;

	strcpy(rel->tuples[rel->relsize], tuple);
	rel->relsize++;
	return 1;
}

// Function: removeTupleAt()
// Purpose: Removes a tuple from a relation by shifting later tuples left
// Parameters: relation_t *rel - relation to modify
//             int index - tuple position to delete
void removeTupleAt(relation_t *rel, int index) {
	for (int i = index; i < rel->relsize - 1; i++)
		strcpy(rel->tuples[i], rel->tuples[i + 1]);

	if (rel->relsize > 0) {
		rel->relsize--;
		rel->tuples[rel->relsize][0] = '\0';
	}
}

// Function: tuplesMatchKey()
// Purpose: Checks whether two tuples have the same key in a relation
// Parameters: relation_t *rel - relation that defines the key fields
//             char *left - first tuple
//             char *right - second tuple
// Returns: 1 if the keys match and 0 otherwise
int tuplesMatchKey(relation_t *rel, char *left, char *right) {
	char leftfields[MAXATTR][RECSIZE];
	char rightfields[MAXATTR][RECSIZE];
	int leftcount = splitTuple(left, leftfields);
	int rightcount = splitTuple(right, rightfields);
	int keycount = rel->keysize;

	if (leftcount < keycount || rightcount < keycount)
		return 0;

	for (int i = 0; i < keycount; i++) {
		if (strcmp(leftfields[i], rightfields[i]) != 0)
			return 0;
	}

	return 1;
}

// Function: tuplesEqual()
// Purpose: Checks whether two tuples are identical
// Parameters: char *left - first tuple
//             char *right - second tuple
// Returns: 1 if they are identical and 0 otherwise
int tuplesEqual(char *left, char *right) {
	return strcmp(left, right) == 0;
}

// Function: resetScan()
// Purpose: Resets the sequential scan position for a relation
// Parameter: char *relname - relation to reset
void resetScan(char *relname) {
	relation_t *rel = getRelation(relname);

	if (rel != NULL)
		rel->scanpos = 0;
}

// Function: syncRelation()
// Purpose: Writes a relation's header and tuple blocks onto the simulated disk
// Parameter: relation_t *rel - relation to flush to disk
void syncRelation(relation_t *rel) {
	int neededblocks = (rel->relsize + BLKFAC - 1) / BLKFAC;
	int tupleindex = 0;
	int headerbuf[BLKSIZE / (int)sizeof(int)];

	while (rel->datablockcount > neededblocks) {
		int blocknum = rel->datablocks[rel->datablockcount - 1];
		char zeros[BLKSIZE];

		memset(zeros, 0, sizeof(zeros));
		diskwrite(blocknum, zeros);
		freeBlock(blocknum);
		rel->datablocks[rel->datablockcount - 1] = -1;
		rel->datablockcount--;
	}

	while (rel->datablockcount < neededblocks) {
		int blocknum = findFreeBlock();

		if (blocknum == -1)
			break;

		rel->datablocks[rel->datablockcount] = blocknum;
		rel->datablockcount++;
	}

	for (int i = 0; i < rel->datablockcount; i++) {
		slot_t block[BLKFAC];
		resetBlock(block);

		for (int j = 0; j < BLKFAC && tupleindex < rel->relsize; j++) {
			block[j].flag = 1;
			strcpy(block[j].tuple, rel->tuples[tupleindex]);
			tupleindex++;
		}

		diskwrite(rel->datablocks[i], (char *) block);
	}

	for (int i = 0; i < BLKSIZE / (int)sizeof(int); i++)
		headerbuf[i] = -1;

	for (int i = 0; i < rel->datablockcount; i++)
		headerbuf[i] = rel->datablocks[i];

	diskwrite(rel->relptr, (char *) headerbuf);
	diskwrite(0, bitmapblk);
}

// Function: syncAllRelations()
// Purpose: Flushes every active relation to the simulated disk
void syncAllRelations() {
	for (int i = 0; i < numrels; i++)
		syncRelation(&relations[i]);
}

// Function: rebuildDictionary()
// Purpose: Recomputes the catalog and columns relations from all current metadata
void rebuildDictionary() {
	relation_t *catalogrel = getRelation("catalog");
	relation_t *columnsrel = getRelation("columns");
	int totalrels = numrels;
	int totalattrs = 0;
	char tuple[RECSIZE];

	if (catalogrel == NULL || columnsrel == NULL)
		return;

	for (int i = 0; i < numrels; i++)
		totalattrs += relations[i].attsize;

	catalogrel->relsize = 0;
	columnsrel->relsize = 0;
	catalogrel->scanpos = 0;
	columnsrel->scanpos = 0;

	for (int i = 0; i < numrels; i++) {
		int relsize = relations[i].relsize;

		if (strcmp(relations[i].relname, "catalog") == 0)
			relsize = totalrels;
		else if (strcmp(relations[i].relname, "columns") == 0)
			relsize = totalattrs;

		snprintf(tuple, sizeof(tuple), "%s %d %d %d %d %d",
				 relations[i].relname,
				 relations[i].kind,
				 relations[i].attsize,
				 relations[i].keysize,
				 relsize,
				 relations[i].relptr);
		appendTuple(catalogrel, tuple);
	}

	for (int i = 0; i < numrels; i++) {
		for (int j = 0; j < relations[i].attsize; j++) {
			snprintf(tuple, sizeof(tuple), "%s %s %d %d",
					 relations[i].relname,
					 relations[i].attrnames[j],
					 relations[i].attrdomains[j],
					 j);
			appendTuple(columnsrel, tuple);
		}
	}
}

// Function: dbcreate()
// Purpose: initializes the specified header buffer and associates with the given header block.
// Parameters: char *relname - name of the relation
//             int kind - 0 for base and 1 for derived
//             int attsize - number of attributes
//             int keysize - number of key fields
//             char attrnames[][9] - attribute names
//             int attrdomains[] - attribute domains
// Returns: the relation header block number or -1 on failure
int dbcreate(char *relname, int kind, int attsize, int keysize,
			 char attrnames[][ATTNAMELEN], int attrdomains[]) {
	int headerblknum;
	relation_t *rel;

	if (numrels >= MAXRELS)
		return -1;

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

	rel = &relations[numrels];
	strcpy(rel->relname, relname);
	rel->kind = kind;
	rel->attsize = attsize;
	rel->keysize = keysize;
	rel->relsize = 0;
	rel->relptr = headerblknum;
	rel->scanpos = 0;
	rel->datablockcount = 0;

	for (int i = 0; i < MAXBLOCKS; i++)
		rel->datablocks[i] = -1;

	for (int i = 0; i < attsize; i++) {
		strcpy(rel->attrnames[i], attrnames[i]);
		rel->attrdomains[i] = attrdomains[i];
	}

	numrels++;
	syncRelation(rel);
	return headerblknum;
}

// Function: dbopen()
// Purpose: Reads the header block of the relation into the given buffer
// Parameters: char *relname - relation to open
//             char *headerblk - buffer that receives the header data
void dbopen(char *relname, char *headerblk) {
	relation_t *rel = getRelation(relname);

	if (rel == NULL)
		return;

	diskread(rel->relptr, headerblk);
}

// Function: dbread()
// Purpose: Reads the next tuple during a sequential scan
// Parameters: char *relname - relation being scanned
//             char *tuple - output buffer for the next tuple
// Returns: 0 at end of relation and non-zero when a tuple was read
int dbread(char *relname, char *tuple) {
	relation_t *rel = getRelation(relname);

	if (rel == NULL)
		return 0;

	if (rel->scanpos >= rel->relsize)
		return 0;

	strcpy(tuple, rel->tuples[rel->scanpos]);
	rel->scanpos++;
	return 1;
}

// Function: dbwrite()
// Purpose: Writes the next tuple into a derived relation
// Parameters: char *relname - relation receiving the tuple
//             char *tuple - tuple to write
// Returns: 1 on success and 0 on failure
int dbwrite(char *relname, char *tuple) {
	relation_t *rel = getRelation(relname);

	if (rel == NULL)
		return 0;

	if (!appendTuple(rel, tuple))
		return 0;

	syncRelation(rel);
	return 1;
}

// Function: dbget()
// Purpose: Looks up a tuple in a relation using the relation's key
// Parameters: char *relname - relation to search
//             char *tuple - key tuple in and matching tuple out
// Returns: 0 if no matching tuple was found and non-zero otherwise
int dbget(char *relname, char *tuple) {
	relation_t *rel = getRelation(relname);

	slotnum = -1;

	if (rel == NULL)
		return 0;

	for (int i = 0; i < rel->relsize; i++) {
		int match;

		if (rel->kind == 0)
			match = tuplesMatchKey(rel, rel->tuples[i], tuple);
		else
			match = tuplesEqual(rel->tuples[i], tuple);

		if (match) {
			strcpy(tuple, rel->tuples[i]);
			slotnum = i;
			return 1;
		}
	}

	return 0;
}

// Function: dbput()
// Purpose: Inserts a tuple into a base relation
// Parameters: char *relname - relation receiving the tuple
//             char *tuple - tuple to insert
void dbput(char *relname, char *tuple) {
	relation_t *rel = getRelation(relname);

	if (rel == NULL) {
		printf("dbput error: relation '%s' not found\n", relname);
		return;
	}

	if (!appendTuple(rel, tuple)) {
		printf("dbput error: block overflow for relation '%s'\n", relname);
		return;
	}

	syncRelation(rel);
}

// Function: dbupdate()
// Purpose: Updates a tuple already found with dbget()
// Parameters: char *relname - relation to modify
//             int slotnum - tuple position to overwrite
//             char *tuple - replacement tuple
void dbupdate(char *relname, int slotnum, char *tuple) {
	relation_t *rel = getRelation(relname);

	if (rel == NULL || slotnum < 0 || slotnum >= rel->relsize)
		return;

	strcpy(rel->tuples[slotnum], tuple);
	syncRelation(rel);
}

// Function: dbremove()
// Purpose: Deletes a tuple already found with dbget()
// Parameters: char *relname - relation to modify
//             int slotnum - tuple position to delete
void dbremove(char *relname, int slotnum) {
	relation_t *rel = getRelation(relname);

	if (rel == NULL || slotnum < 0 || slotnum >= rel->relsize)
		return;

	removeTupleAt(rel, slotnum);
	syncRelation(rel);
}

// Function: deleteRelationAt()
// Purpose: Removes a relation from memory and frees its disk blocks
// Parameter: int index - array position of the relation to remove
void deleteRelationAt(int index) {
	relation_t *rel = &relations[index];

	for (int i = 0; i < rel->datablockcount; i++) {
		char zeros[BLKSIZE];

		memset(zeros, 0, sizeof(zeros));
		diskwrite(rel->datablocks[i], zeros);
		freeBlock(rel->datablocks[i]);
	}

	if (!isDictionaryRelation(rel->relname)) {
		char zeros[BLKSIZE];

		memset(zeros, 0, sizeof(zeros));
		diskwrite(rel->relptr, zeros);
		freeBlock(rel->relptr);
	}

	for (int i = index; i < numrels - 1; i++)
		relations[i] = relations[i + 1];

	numrels--;
}

// Function: dbclose()
// Purpose: flushes the header and data blocks for a relation
// Parameter: char *relname - relation to flush
void dbclose(char *relname) {
	relation_t *rel = getRelation(relname);

	if (rel != NULL)
		syncRelation(rel);
}

// Function: dbdelete()
// Purpose: Deletes a relation and frees all storage owned by it
// Parameter: char *relname - relation to delete
void dbdelete(char *relname) {
	int index = getRelationIndex(relname);

	if (index == -1)
		return;

	if (isDictionaryRelation(relname)) {
		printf("DELETE error: cannot delete dictionary relation '%s'\n", relname);
		return;
	}

	deleteRelationAt(index);
	rebuildDictionary();
	syncAllRelations();
}

// Function: canCreateBaseRelation()
// Purpose: Checks whether a new base relation name is legal
// Parameter: char *relname - proposed relation name
// Returns: 1 if it is legal and 0 otherwise
int canCreateBaseRelation(char *relname) {
	if (isDictionaryRelation(relname))
		return 0;

	if (getRelation(relname) != NULL)
		return 0;

	return 1;
}

// Function: prepareDerivedRelation()
// Purpose: Prepares the result relation for an algebra operator
// Parameters: char *relname - result relation name
//             int attsize - result attribute count
//             char attrnames[][9] - result attribute names
//             int attrdomains[] - result attribute domains
// Returns: 1 on success and 0 if the relation cannot be created
int prepareDerivedRelation(char *relname, int attsize,
						   char attrnames[][ATTNAMELEN], int attrdomains[]) {
	relation_t *existing = getRelation(relname);

	if (isDictionaryRelation(relname))
		return 0;

	if (existing != NULL) {
		if (existing->kind == 0)
			return 0;
		dbdelete(relname);
	}

	return dbcreate(relname, 1, attsize, attsize, attrnames, attrdomains) != -1;
}

// Function: schemasCompatible()
// Purpose: Checks whether two relations are union/difference compatible
// Parameters: relation_t *left - first relation
//             relation_t *right - second relation
//             int rightmap[] - output mapping from left columns to right columns
// Returns: 1 if compatible and 0 otherwise
int schemasCompatible(relation_t *left, relation_t *right, int rightmap[]) {
	if (left->attsize != right->attsize)
		return 0;

	for (int i = 0; i < left->attsize; i++) {
		int found = -1;

		for (int j = 0; j < right->attsize; j++) {
			if (strcmp(left->attrnames[i], right->attrnames[j]) == 0 &&
				left->attrdomains[i] == right->attrdomains[j]) {
				found = j;
				break;
			}
		}

		if (found == -1)
			return 0;

		rightmap[i] = found;
	}

	return 1;
}

// Function: reorderTuple()
// Purpose: Reorders a tuple from one schema into another schema order
// Parameters: char *outtuple - reordered output tuple
//             char *intuple - source tuple
//             int map[] - source positions for each output position
//             int count - number of attributes to reorder
void reorderTuple(char *outtuple, char *intuple, int map[], int count) {
	char infields[MAXATTR][RECSIZE];
	char outfields[MAXATTR][RECSIZE];

	splitTuple(intuple, infields);

	for (int i = 0; i < count; i++)
		strcpy(outfields[i], infields[map[i]]);

	buildTuple(outtuple, outfields, count);
}

// Function: printHeaders()
// Purpose: Prints the attribute names for a relation
// Parameter: relation_t *rel - relation whose schema is being printed
void printHeaders(relation_t *rel) {
	for (int i = 0; i < rel->attsize; i++) {
		if (i != 0)
			printf(" ");
		printf("%s", rel->attrnames[i]);
	}
	printf("\n");
}

// Function: compareField()
// Purpose: Compares one tuple field against a selection condition
// Parameters: int domain - 0 for string and 1 for integer
//             char *left - tuple field value
//             char *op - comparison operator
//             char *right - condition value
// Returns: 1 if the condition holds and 0 otherwise
int compareField(int domain, char *left, char *op, char *right) {
	int cmp;

	if (domain == 1) {
		int lval = atoi(left);
		int rval = atoi(right);

		if (strcmp(op, "==") == 0)
			return lval == rval;
		if (strcmp(op, "!=") == 0)
			return lval != rval;
		if (strcmp(op, ">") == 0)
			return lval > rval;
		if (strcmp(op, ">=") == 0)
			return lval >= rval;
		if (strcmp(op, "<") == 0)
			return lval < rval;
		if (strcmp(op, "<=") == 0)
			return lval <= rval;
		return 0;
	}

	cmp = strcmp(left, right);

	if (strcmp(op, "==") == 0)
		return cmp == 0;
	if (strcmp(op, "!=") == 0)
		return cmp != 0;
	if (strcmp(op, ">") == 0)
		return cmp > 0;
	if (strcmp(op, ">=") == 0)
		return cmp >= 0;
	if (strcmp(op, "<") == 0)
		return cmp < 0;
	if (strcmp(op, "<=") == 0)
		return cmp <= 0;

	return 0;
}

// Function: do_create()
// Purpose: Handles the CR command for base relation creation
void do_create() {
	char relname[RELNAMELEN];
	int n, k;
	int attrdomains[MAXATTR];
	char attrnames[MAXATTR][ATTNAMELEN];

	scanf("%31s %d %d", relname, &n, &k);

    // read n attribute specs
    for (int i = 0; i < n; i++) {
        char domain[2];
        scanf("%19s %1s", attrnames[i], domain);
		if (domain[0] == 'I')
			attrdomains[i] = 1;
		else
			attrdomains[i] = 0;
    }

	if (!canCreateBaseRelation(relname)) {
		printf("CREATE error: illegal relation name '%s'\n", relname);
		return;
	}

	if (dbcreate(relname, 0, n, k, attrnames, attrdomains) == -1)
		return;

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_delete()
// Purpose: Handles the DE command for deleting a relation
void do_delete() {
    char relname[RELNAMELEN];

    scanf("%31s", relname);
    dbdelete(relname);
}

// Function: do_insert()
// Purpose: Handles the IN command for inserting tuples into a base relation
void do_insert() {
    char relname[RELNAMELEN];
    int n;
    char tuple[RECSIZE];
	relation_t *rel;
	int c;

    scanf("%31s %d", relname, &n);
	rel = getRelation(relname);

    while ((c = getchar()) != '\n' && c != EOF) {
		/* remove leftover newline */
	}

	if (rel == NULL || rel->kind != 0 || isDictionaryRelation(relname)) {
		for (int i = 0; i < n; i++)
			fgets(tuple, RECSIZE, stdin);
		return;
	}

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

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_remove()
// Purpose: Handles the RM command for deleting tuples from a base relation
void do_remove() {
    char relname[RELNAMELEN];
    int n;
    char tuple[RECSIZE];
	relation_t *rel;
    int c;

    scanf("%31s %d", relname, &n);
	rel = getRelation(relname);

    while ((c = getchar()) != '\n' && c != EOF) {
	}

	if (rel == NULL || rel->kind != 0 || isDictionaryRelation(relname)) {
		for (int i = 0; i < n; i++)
			fgets(tuple, RECSIZE, stdin);
		return;
	}

    for (int i = 0; i < n; i++) {
        fgets(tuple, RECSIZE, stdin);
		tuple[strcspn(tuple, "\n")] = '\0';
        if (dbget(relname, tuple) == 0) {
            printf("REMOVE error: key not found in relation '%s'\n", relname);
            continue;
        }
        dbremove(relname, slotnum);
    }

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_update()
// Purpose: Handles the UP command for replacing tuples in a base relation
void do_update() {
    char relname[RELNAMELEN];
    int n;
    char tuple[RECSIZE];
	char newtuple[RECSIZE];
	relation_t *rel;
	int c;

    scanf("%31s %d", relname, &n);
	rel = getRelation(relname);

    while ((c = getchar()) != '\n' && c != EOF) {
	}

	if (rel == NULL || rel->kind != 0 || isDictionaryRelation(relname)) {
		for (int i = 0; i < n; i++)
			fgets(tuple, RECSIZE, stdin);
		return;
	}

    for (int i = 0; i < n; i++) {
        fgets(tuple, RECSIZE, stdin);
		tuple[strcspn(tuple, "\n")] = '\0';
		strcpy(newtuple, tuple);
        if (dbget(relname, tuple) == 0) {
            printf("UPDATE error: key not found in relation '%s'\n", relname);
            continue;
        }
        dbupdate(relname, slotnum, newtuple);
    }

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_print()
// Purpose: Handles the PR command for printing a relation
void do_print() {
    char relname[RELNAMELEN];
    char tuple[RECSIZE];
	relation_t *rel;

    scanf("%31s", relname);
	rel = getRelation(relname);

	if (rel == NULL)
		return;

    printf("%s\n", relname);
    printHeaders(rel);

	resetScan(relname);
    while (dbread(relname, tuple) != 0)
        printf("%s\n", tuple);
}

// Function: do_union()
// Purpose: Handles the UN command for creating the union of two relations
void do_union() {
    char p[RELNAMELEN], q[RELNAMELEN], r[RELNAMELEN];
	char tuple[RECSIZE];
	char reordered[RECSIZE];
	int rightmap[MAXATTR];
	relation_t *prel;
	relation_t *qrel;

    scanf("%31s %31s %31s", p, q, r);
	prel = getRelation(p);
	qrel = getRelation(q);

	if (prel == NULL || qrel == NULL)
		return;

	if (!schemasCompatible(prel, qrel, rightmap))
		return;

	if (!prepareDerivedRelation(r, prel->attsize, prel->attrnames, prel->attrdomains))
		return;

    // add all tuples from p
	resetScan(p);
    while (dbread(p, tuple) != 0)
        dbwrite(r, tuple);

    // add tuples from q only if not already in r
	resetScan(q);
    while (dbread(q, tuple) != 0) {
		reorderTuple(reordered, tuple, rightmap, prel->attsize);
        if (dbget(r, reordered) == 0)
            dbwrite(r, reordered);
    }

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_difference()
// Purpose: Handles the DF command for computing set difference
void do_difference() {
    char p[RELNAMELEN], q[RELNAMELEN], r[RELNAMELEN];
    char tuple[RECSIZE];
	char qtuple[RECSIZE];
	int rightmap[MAXATTR];
	relation_t *prel;
	relation_t *qrel;

    scanf("%31s %31s %31s", p, q, r);
	prel = getRelation(p);
	qrel = getRelation(q);

	if (prel == NULL || qrel == NULL)
		return;

	if (!schemasCompatible(prel, qrel, rightmap))
		return;

	if (!prepareDerivedRelation(r, prel->attsize, prel->attrnames, prel->attrdomains))
		return;

    resetScan(p);
    while (dbread(p, tuple) != 0) {
		int found = 0;

		resetScan(q);
		while (dbread(q, qtuple) != 0) {
			char reordered[RECSIZE];
			reorderTuple(reordered, qtuple, rightmap, prel->attsize);
			if (strcmp(reordered, tuple) == 0) {
				found = 1;
				break;
			}
		}

        if (!found)
            dbwrite(r, tuple);
    }

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_projection()
// Purpose: Handles the PJ command for projecting attributes
void do_projection() {
    char p[RELNAMELEN], q[RELNAMELEN];
    int n;
    char tuple[RECSIZE];
    char projected[RECSIZE];
	char attrnames[MAXATTR][ATTNAMELEN];
	int attrdomains[MAXATTR];
	int attrindexes[MAXATTR];
	relation_t *prel;

    scanf("%31s %31s %d", p, q, &n);
	prel = getRelation(p);

    for (int i = 0; i < n; i++) {
        scanf("%19s", attrnames[i]);
	}

	if (prel == NULL)
		return;

	for (int i = 0; i < n; i++) {
		attrindexes[i] = getAttrIndex(prel, attrnames[i]);
		if (attrindexes[i] == -1)
			return;
		attrdomains[i] = prel->attrdomains[attrindexes[i]];
	}

	if (!prepareDerivedRelation(q, n, attrnames, attrdomains))
		return;

    resetScan(p);
    while (dbread(p, tuple) != 0) {
        // extract only the specified attributes
        char fields[MAXATTR][RECSIZE];
        char outfields[MAXATTR][RECSIZE];

		splitTuple(tuple, fields);
		for (int i = 0; i < n; i++)
			strcpy(outfields[i], fields[attrindexes[i]]);
		buildTuple(projected, outfields, n);

        // then suppress duplicates
        if (dbget(q, projected) == 0)
            dbwrite(q, projected);
    }

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_selection()
// Purpose: Handles the SL command for tuple selection
void do_selection() {
    char p[RELNAMELEN], q[RELNAMELEN];
    int n;
    char tuple[RECSIZE];
	char condattrs[MAXATTR][ATTNAMELEN];
    char ops[MAXATTR][3];
    char vals[MAXATTR][RECSIZE];
	int attrindexes[MAXATTR];
	relation_t *prel;

    scanf("%31s %31s %d", p, q, &n);
	prel = getRelation(p);

    for (int i = 0; i < n; i++) {
        scanf("%19s %2s %62s", condattrs[i], ops[i], vals[i]);
	}

	if (prel == NULL)
		return;

	for (int i = 0; i < n; i++) {
		attrindexes[i] = getAttrIndex(prel, condattrs[i]);
		if (attrindexes[i] == -1)
			return;
	}

	if (!prepareDerivedRelation(q, prel->attsize, prel->attrnames, prel->attrdomains))
		return;

    resetScan(p);
    while (dbread(p, tuple) != 0) {
        // check all n conditions against tuple
        // if all pass, write to q
		char fields[MAXATTR][RECSIZE];
		int pass = 1;

		splitTuple(tuple, fields);
		for (int i = 0; i < n; i++) {
			int attrindex = attrindexes[i];
			if (!compareField(prel->attrdomains[attrindex], fields[attrindex], ops[i], vals[i])) {
				pass = 0;
				break;
			}
		}

		if (pass)
        	dbwrite(q, tuple);
    }

	rebuildDictionary();
	syncAllRelations();
}

// Function: do_naturaljoin()
// Purpose: Handles the NJ command for natural join
void do_naturaljoin() {
    char p[RELNAMELEN], q[RELNAMELEN], r[RELNAMELEN];
    int n;
	char ptuple[RECSIZE], qtuple[RECSIZE];
	char attrnames[MAXATTR][ATTNAMELEN];
	char resultattrs[MAXATTR][ATTNAMELEN];
	int resultdomains[MAXATTR];
	int joinp[MAXATTR];
	int joinq[MAXATTR];
	int qinclude[MAXATTR];
	int resultattsize = 0;
	relation_t *prel;
	relation_t *qrel;

    scanf("%31s %31s %31s %d", p, q, r, &n);
	prel = getRelation(p);
	qrel = getRelation(q);

    for (int i = 0; i < n; i++) {
        scanf("%19s", attrnames[i]);
	}

	if (prel == NULL || qrel == NULL)
		return;

	for (int i = 0; i < n; i++) {
		joinp[i] = getAttrIndex(prel, attrnames[i]);
		joinq[i] = getAttrIndex(qrel, attrnames[i]);
		if (joinp[i] == -1 || joinq[i] == -1)
			return;
	}

	for (int i = 0; i < prel->attsize; i++) {
		strcpy(resultattrs[resultattsize], prel->attrnames[i]);
		resultdomains[resultattsize] = prel->attrdomains[i];
		resultattsize++;
	}

	for (int i = 0; i < qrel->attsize; i++) {
		int isjoin = 0;

		for (int j = 0; j < n; j++) {
			if (i == joinq[j]) {
				isjoin = 1;
				break;
			}
		}

		qinclude[i] = !isjoin;
		if (!isjoin) {
			strcpy(resultattrs[resultattsize], qrel->attrnames[i]);
			resultdomains[resultattsize] = qrel->attrdomains[i];
			resultattsize++;
		}
	}

	if (!prepareDerivedRelation(r, resultattsize, resultattrs, resultdomains))
		return;

    resetScan(p);
    while (dbread(p, ptuple) != 0) {
		char pfields[MAXATTR][RECSIZE];
		splitTuple(ptuple, pfields);

		resetScan(q);
        while (dbread(q, qtuple) != 0) {
			char qfields[MAXATTR][RECSIZE];
			char outfields[MAXATTR][RECSIZE];
			char joined[RECSIZE];
			int outcount = 0;
			int match = 1;

			splitTuple(qtuple, qfields);

            // check if common attributes match
			for (int i = 0; i < n; i++) {
				if (strcmp(pfields[joinp[i]], qfields[joinq[i]]) != 0) {
					match = 0;
					break;
				}
			}

            // if yes, combine and write to r
			if (!match)
				continue;

			for (int i = 0; i < prel->attsize; i++) {
				strcpy(outfields[outcount], pfields[i]);
				outcount++;
			}

			for (int i = 0; i < qrel->attsize; i++) {
				if (qinclude[i]) {
					strcpy(outfields[outcount], qfields[i]);
					outcount++;
				}
			}

			buildTuple(joined, outfields, outcount);
			dbwrite(r, joined);
        }
    }

	rebuildDictionary();
	syncAllRelations();
}

int main(void) {
	char cmd[3];
	char catalogattrs[6][ATTNAMELEN] = {
		"Relname", "Kind", "Attsize", "Keysize", "Relsize", "Relptr"
	};
	int catalogdomains[6] = {0, 1, 1, 1, 1, 1};
	char columnsattrs[4][ATTNAMELEN] = {
		"Relname", "Attname", "Attdomain", "Attposition"
	};
	int columnsdomains[4] = {0, 0, 1, 1};

	initDisk();
	memset(bitmapblk, 0, sizeof(bitmapblk));

	bitmapblk[0] = 1;  // bitmap block
	bitmapblk[1] = 1;  // catalog header
	bitmapblk[2] = 1;  // columns header

	// initialize headerblocks
	dbcreate("catalog", 0, 6, 1, catalogattrs, catalogdomains);
	dbcreate("columns", 0, 4, 2, columnsattrs, columnsdomains);
	rebuildDictionary();
	syncAllRelations();

	while (scanf("%2s", cmd) == 1) {
		if (strcmp(cmd, "CR") == 0)
			do_create();
		else if (strcmp(cmd, "DE") == 0)
			do_delete();
		else if (strcmp(cmd, "IN") == 0)
			do_insert();
		else if (strcmp(cmd, "RM") == 0)
			do_remove();
		else if (strcmp(cmd, "UP") == 0)
			do_update();
		else if (strcmp(cmd, "PR") == 0)
			do_print();
		else if (strcmp(cmd, "UN") == 0)
			do_union();
		else if (strcmp(cmd, "DF") == 0)
			do_difference();
		else if (strcmp(cmd, "PJ") == 0)
			do_projection();
		else if (strcmp(cmd, "SL") == 0)
			do_selection();
		else if (strcmp(cmd, "NJ") == 0)
			do_naturaljoin();
	}

	for (int i = 0; i < numrels; i++)
		dbclose(relations[i].relname);

	return 0;
}

