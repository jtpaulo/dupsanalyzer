/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <openssl/sha.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include "berk.c"
#include "rabin_polynomial.c"

//max path size of a folder/file
#define MAXSIZEP 10000
#define FOLDER 1
#define DEVICE 2

#define FIXEDBLOCKS 0
#define RABIN 1

//TODO for now this is static
#define READSIZE 5242880


//TODO define these new variables
int nr_sizes_proc=0;
int *sizes_proc;

int nr_sizes_proc_rabin=0;
int *sizes_proc_rabin;
//Rabin current block being processed for each avg block size
struct rab_block_info **cur_block;


//TODO in the future this will be an argument
#define PRINTDB "gendbs/printdb/"
#define DUPLICATEDB "gendbs/duplicatedb/"

//blocks avoided when EOF is reached and the size of the block does not fit
uint64_t *incomplete_blocks;

//space consumed by incomplete blocks
uint64_t *incomplete_space;


//total blocks scanned
uint64_t *total_blocks;
//identical blocks (that could be eliminated)
uint64_t *eq;
//distinct blocks
uint64_t *dif;
//distinct blocks with duplicates
uint64_t *distinctdup;
//blocks that were appended with zeros due to their size
//uint64_t *zeroed_blocks;

//duplicated disk space
uint64_t *space;


//number files scaneed
uint64_t nfiles=0;

//This is the processed blocks of READSIZE so it is not an array
uint64_t processed_blocks=0;

//duplicates DB
DB ***dbporiginal; // DB structure handle
DB_ENV ***envporiginal;


//print file DB
DB ***dbprinter; // DB structure handle
DB_ENV ***envprinter;


//calculate block checksum and see if already exists at the hash table
int check_duplicates(unsigned char* block,uint64_t bsize,int id_blocksize){

  //calculate sha1 hash
  unsigned char *result = malloc(sizeof(unsigned char)*20);
  result[0]=0;
  SHA1(block, bsize,result);

  //generate a visual (ASCII) representation for the hashes
    char *start=malloc(sizeof(char)*41);
    start[40]=0;
    int j =0;

    for(j=0;j<40;j+=2)
    {
	start[j]=(result[j/2]&0x0f)+'a';
	start[j+1]=((result[j/2]&0xf0)>>4)+'a';
    }

    free(result);

    //one more block scanned
    total_blocks[id_blocksize]++;

    //get hash from berkley db
    struct hash_value hvalue;

    //printf("before introducing in db\n");
    int ret = get_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);
    

    //if hash entry does not exist
    if(ret == DB_NOTFOUND){
    	// printf("not found\n");

       //unique blocks++
       dif[id_blocksize]++;

       //set counter to 1
       hvalue.cont=1;

       //insert into into the hashtable
       put_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);
    }
    else{

    	// printf("after search\n");

    	if(hvalue.cont==1){
    		//this is a distinct block with duplicate
    		distinctdup[id_blocksize]++;
    	}

    	//found duplicated block
        eq[id_blocksize]++;
    	//space that could be spared
    	space[id_blocksize]=space[id_blocksize]+bsize;

    	//increase counter
        hvalue.cont++;

        //insert counter in the right entry
        put_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);

    }

    free(start);

    processed_blocks++;
    if(processed_blocks%100000==0){
    	printf("processed %llu blocks\n",(long long unsigned int) processed_blocks);
    }

  return 0;
}

//calculate block checksum and see if already exists at the hash table
int check_rabin_duplicates(struct rabin_polynomial *poly,int id_blocksize){

	/*FILE *fp;
	char namelog[100];
	char ids[10];
	sprintf(ids,"%d",id_blocksize);
	strcpy(namelog,"output");
	strcat(namelog,ids);
	fp=fopen(namelog,"a");
*/


  //generate a visual (ASCII) representation for the hashes
    char start[41];
    start[40]=0;
    bzero(start,sizeof(char)*41);

    char polys[21];
    bzero(polys,sizeof(char)*21);
    sprintf(polys,"%llu",(unsigned long long int) poly->polynomial);

    char sizepol[18];
    bzero(sizepol,sizeof(char)*18);
    sprintf(sizepol,"%d",poly->length);


    strcpy(start,polys);
    strcat(start,sizepol);

    /*fprintf(fp,"%s %s %s\n",polys,sizepol,start);
    fclose(fp);*/

   // printf("%d %llu\n",poly->length,poly->polynomial);

    //one more block scanned
    total_blocks[id_blocksize]++;

    //get hash from berkley db
    struct hash_value hvalue;

    //printf("before introducing in db\n");
    int ret = get_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);


    //if hash entry does not exist
    if(ret == DB_NOTFOUND){
    	// printf("not found\n");

       //unique blocks++
       dif[id_blocksize]++;

       //set counter to 1
       hvalue.cont=1;

       //insert into into the hashtable
       put_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);
    }
    else{

    	// printf("after search\n");

    	if(hvalue.cont==1){
    		//this is a distinct block with duplicate
    		distinctdup[id_blocksize]++;
    	}

    	//found duplicated block
        eq[id_blocksize]++;
    	//space that could be spared
    	space[id_blocksize]=space[id_blocksize]+poly->length;

    	//increase counter
        hvalue.cont++;

        //insert counter in the right entry
        put_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);

    }

    free(poly);




  return 0;
}

//given a file extract blocks and check for duplicates
int extract_blocks(char* filename){

	printf("Processing %s \n",filename);
    int fd = open(filename,O_RDONLY | O_LARGEFILE);
    uint64_t off=0;
    if(fd){

      //declare a block
      unsigned char block_read[READSIZE];
      bzero(block_read,sizeof(block_read));

      //read first block from file
      int aux = pread(fd,block_read,READSIZE,off);
      off+= READSIZE;

      //check if the file still has more blocks and if size <bzise discard
      //the last incomplete block
      while(aux>0){

      	//Process fixed size dups all sizes specified
         int curr_sizes_proc=0;
         while(curr_sizes_proc<nr_sizes_proc){

        	 int size_block=sizes_proc[curr_sizes_proc];
        	 int size_proced=0;
        	 unsigned char *block_proc;

           
        	 while(aux-size_proced>=size_block){

        		 block_proc=malloc(size_block);
        		 bzero(block_proc,size_block);

        		 memcpy(block_proc,&block_read[size_proced],size_block);

        		 //index the block and find duplicates
        		 check_duplicates(block_proc,size_block,curr_sizes_proc);

        		 free(block_proc);

        		 size_proced+=size_block;
        	 }


           if(aux-size_proced > 0){
            incomplete_blocks[curr_sizes_proc]++;
            incomplete_space[curr_sizes_proc]+=aux-size_proced;
           }


        	 curr_sizes_proc++;
       	 }


       	 //process rabin chunks with avg size of 1K,4K,8K,16K
         //min size = avg size/2  and max size = avg size*2
         //Process fixed size dups for 1K,4K,8K,16K
       	 int curr_sizes_proc_rabin=0;
         while(curr_sizes_proc_rabin<nr_sizes_proc_rabin){

           rabin_polynomial_min_block_size=sizes_proc_rabin[curr_sizes_proc_rabin]/2;
           rabin_polynomial_max_block_size=sizes_proc_rabin[curr_sizes_proc_rabin]*2;
           rabin_polynomial_average_block_size=sizes_proc_rabin[curr_sizes_proc_rabin];
           rabin_sliding_window_size=30;

           struct rab_block_info *block;

           if(cur_block[curr_sizes_proc_rabin] == NULL) {

        	  printf("Initializing rabin block %d\n",curr_sizes_proc_rabin);
              cur_block[curr_sizes_proc_rabin]=init_empty_block();
           }

           block=cur_block[curr_sizes_proc_rabin];

           int i;
           for(i=0;i<aux;i++) {
               	   char cur_byte=*((char *)(block_read+i));
                   char pushed_out=block->current_window_data[block->window_pos];
                   block->current_window_data[block->window_pos]=cur_byte;
                   block->cur_roll_checksum=(block->cur_roll_checksum*rabin_polynomial_prime)+cur_byte;
                   block->tail->polynomial=(block->tail->polynomial*rabin_polynomial_prime)+cur_byte;
                   block->cur_roll_checksum-=(pushed_out*polynomial_lookup_buf[rabin_sliding_window_size]);

                   block->window_pos++;
                   block->total_bytes_read++;
                   block->tail->length++;

                   if(block->window_pos == rabin_sliding_window_size) //Loop back around
                       block->window_pos=0;


                   //If we hit our special value or reached the max win size create a new block
                   if((block->tail->length >= rabin_polynomial_min_block_size && (block->cur_roll_checksum % rabin_polynomial_average_block_size) == rabin_polynomial_prime)|| block->tail->length == rabin_polynomial_max_block_size) {


                	   block->tail->start=block->total_bytes_read-block->tail->length;

                	   //insert hash in berkDB
                	   //index the block and find duplicates
                	   check_rabin_duplicates(cur_block[curr_sizes_proc_rabin]->tail,nr_sizes_proc+curr_sizes_proc_rabin);


                	   //free oldblock and polinomial
                	   //free(cur_block[curr_sizes_proc]->tail);
                	   cur_block[curr_sizes_proc_rabin]->tail=gen_new_polynomial(NULL,0,0,0);

                       //if(i==READSIZE-1)
                    	   //cur_block[curr_sizes_proc_rabin]->current_poly_finished=1;
                   }


           }

           curr_sizes_proc_rabin++;

         }

       	 //free this block from memory and process another
         //free(block_read);
         //block_read=malloc(sizeof(unsigned char)*READSIZE);
         aux = pread(fd,block_read,READSIZE,off);
         off+=READSIZE;



      }

    //zero the blocks a new file is being processed 
    int auxc=0;
    for(auxc=0;auxc<nr_sizes_proc_rabin;auxc++){

        struct rab_block_info *lastblock;
        lastblock = cur_block[auxc];

        if( lastblock->tail->length > 0){
            incomplete_blocks[nr_sizes_proc+auxc]++;
            incomplete_space[nr_sizes_proc+auxc]+=lastblock->tail->length;
        }

        cur_block[auxc]=NULL;
    } 

    close(fd);
    }
    else{
      fprintf(stderr,"error opening file %s\n",filename);
      exit(1);

    }

  return 0;

}

//search a directory for files inside and return the number of files found and their path(nfiles,lfiles)
int search_dir(char* path){

 //directory information
 struct dirent *dp=malloc(sizeof(struct dirent));

 //opens the path and check the files inside
 DIR *dirp = opendir(path);
 if(dirp){
	//while opendir is not null
    while ((dp = readdir(dirp)) != NULL){
      //exclude . and ..
      if(strcmp(dp->d_name,".")!=0 && strcmp(dp->d_name,"..")!=0){

         //build the full path
         char newpath[MAXSIZEP];

         strcpy(newpath,path);
         strcat(newpath,dp->d_name);

         if(dp->d_type==DT_DIR){
               strcat(newpath,"/");
               // recursively process the files inside the diretory
               search_dir(newpath);
         }
         else{
            if(dp->d_type==DT_REG){
            	//If it is a regular file then start segmenting files and indexing blocks

            	extract_blocks(newpath);
            	nfiles++;

            }

         }
      }
    }
 }
 closedir(dirp);
 free(dp);

 //printf("return search dir\n");

 return 0;
}



int gen_output(DB **dbpor,DB_ENV **envpor,DB **dbprint,DB_ENV **envprint){

	//Iterate through original DB and insert in print DB
	int ret;

	DBT key, data;

	DBC *cursorp;

	(*dbpor)->cursor(*dbpor, NULL, &cursorp, 0);

	// Initialize our DBTs.
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	// Iterate over the database, retrieving each record in turn.
	while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {

	   //get hash from berkley db
	   struct hash_value hvalue;
	   uint64_t ndups = (unsigned long long int)((struct hash_value *)data.data)->cont;
	   ndups=ndups-1;
	   //char ndups[25];

	   //key
	   //sprintf(ndups,"%llu",(unsigned long long int)((struct hash_value *)data.data)->cont);

	   //see if entry already exists and
	   //Insert new value in hash for print number_of_duplicates->numberof blocks
	   int retprint = get_db_print(&ndups,&hvalue,dbprint,envprint);

	   //if hash entry does not exist
	   if(retprint == DB_NOTFOUND){

		  hvalue.cont=1;
		  //insert into into the hashtable
		  put_db_print(&ndups,&hvalue,dbprint,envprint);
       }
	   else{

		  //increase counter
		  hvalue.cont++;
		  //insert counter in the right entry
		  put_db_print(&ndups,&hvalue,dbprint,envprint);
       }

	}
	if (ret != DB_NOTFOUND) {
	    perror("failed while iterating");
	}


	if (cursorp != NULL)
	    cursorp->close(cursorp);

	return 0;
}


//Aux function to split the string with multiple sizes
void str_split(char* a_str, int type)
{
    char* tmp = a_str;
   
    //starts with one because one element does not need the comma
    while (*tmp)
    {
        if (',' == *tmp)
        {
        	if(type==FIXEDBLOCKS)
            	nr_sizes_proc++;
            else
            	nr_sizes_proc_rabin++;
        }
        tmp++;
    }

  	//increment one more due to the last str
  	if(type==FIXEDBLOCKS){
        nr_sizes_proc++;
        sizes_proc = malloc(sizeof(int)*nr_sizes_proc);
    }
    else{
      	nr_sizes_proc_rabin++;
      	sizes_proc_rabin = malloc(sizeof(int)*nr_sizes_proc_rabin);
     }
    

    char* token = strtok(a_str, ",");

    int i=0;
    while (token)
    {
    	if(type==FIXEDBLOCKS)
        	sizes_proc[i] = atoi(token);	
    	else
      		sizes_proc_rabin[i] = atoi(token);
        
        token = strtok(NULL, ",");
        i++;
    }

    printf("nrsizes rabin %d\n",nr_sizes_proc_rabin);

}

void usage(void)
{
	printf("Usage:\n");
	printf(" -f or -d\t\t\t(Find duplicates in folder -f or in a Disk Device -d)\n");
	printf(" -p<value>\t\t\t(Path for the folder or disk device)\n");
	printf(" -b<value> or/and -r<value>\t (Size of fixed-size blocks or/and average size of Rabin fingerprints to analyze in bytes)");
	printf(" -h\t\t\t\t(Help)\n");
	exit (8);
}

void help(void){

	printf(" Help:\n\n");
	printf(" -f or -d\t\t\t(Find duplicates in folder -f or in a Disk Device -d)\n");
	printf(" -p<value>\t\t\t(Path for the folder or disk device)\n");
	printf(" -b<value> or/and -r<value>\t (Size of fixed-size blocks or/and average size of Rabin fingerprints to analyze in bytes)\n");
	printf("\n\n Optional Parameters\n\n");
	printf(" -o<value>\t\t\t\t(Path for the output distribution file. If not specified this is not generated.)\n");
	printf(" -z<value>\t(Path for the folder where duplicates databases are created default: ./gendbs/duplicatedb/)\n");
	
	exit (8);

}




int main (int argc, char *argv[]){

	//directory or disk device
	int devicetype=-1;
    //path to the device
	char devicepath[100];

	//path of databases folder
	int dbfolder=0;
	char dbfolderpath[100];

	
  	while ((argc > 1) && (argv[1][0] == '-'))
  	{
		switch (argv[1][1])
		{
			case 'f':
			//Test if -d is not being used also
			if(devicetype!=DEVICE)
				devicetype=FOLDER;
			else{
			   printf("Cannot use both -f and -d\n");
			   usage();
			}
			break;
			case 'd':
			//test if -f is not being used also
			if(devicetype!=FOLDER)
				devicetype=DEVICE;
			else{
			    printf("Cannot use both -f and -d\n\n");
			    usage();
			}
			break;
			case 'p':
				strcpy(devicepath,&argv[1][2]);
			break;
			case 'b':
				str_split(&argv[1][2],FIXEDBLOCKS);
				break;
			case 'r':
				str_split(&argv[1][2],RABIN);
				break;
			case 'z':
				dbfolder=1;
				strcpy(dbfolderpath,&argv[1][2]);
				break;
			case 'h':
				help();
				break;
			default:
				printf("Wrong Argument: %s\n", argv[1]);
				usage();
				exit(0);
				break;
			}

			++argv;
			--argc;
	}


	//test if iotype is defined
	if(devicetype!=FOLDER && devicetype!=DEVICE){
		printf("missing -f or -d\n\n");
		usage();
		exit(0);
	}
	//test if testype is defined
	if(strlen(devicepath)==0){
		printf("missing -p<value>\n\n");
		usage();
		exit(0);
	}
	if(nr_sizes_proc_rabin==0 && nr_sizes_proc==0){
		printf("missing -b<value> or -r<value>\n\n");
		usage();
		exit(0);
	}



	//Initialize variables
	total_blocks=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));
	eq=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));

  incomplete_blocks=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));
  incomplete_space=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));

	dif=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));
	distinctdup=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));
	//zeroed_blocks=malloc(sizeof(uint64_t)*nr_sizes_proc);
	space=malloc(sizeof(uint64_t)*(nr_sizes_proc+nr_sizes_proc_rabin));

	dbporiginal=malloc(sizeof(DB**)*(nr_sizes_proc+nr_sizes_proc_rabin));
	envporiginal=malloc(sizeof(DB_ENV**)*(nr_sizes_proc+nr_sizes_proc_rabin));

	dbprinter=malloc(sizeof(DB**)*(nr_sizes_proc+nr_sizes_proc_rabin));
	envprinter=malloc(sizeof(DB_ENV**)*(nr_sizes_proc+nr_sizes_proc_rabin));


	int aux=0;
	for(aux=0;aux<(nr_sizes_proc+nr_sizes_proc_rabin);aux++){

		dbporiginal[aux]=malloc(sizeof(DB *));
		envporiginal[aux]=malloc(sizeof(DB_ENV *));
		dbprinter[aux]=malloc(sizeof(DB *));
		envprinter[aux]=malloc(sizeof(DB_ENV *));

		char printdbpath[100];
		char duplicatedbpath[100];
		char sizeid[5];
		sprintf(sizeid,"%d",aux);


		//if a folder were specified for databases
		if(dbfolder==1){
			strcpy(printdbpath,PRINTDB);
			strcat(printdbpath,sizeid);
			strcpy(duplicatedbpath,dbfolderpath);
			strcat(duplicatedbpath,sizeid);
		}
		else{
			strcpy(printdbpath,PRINTDB);
			strcat(printdbpath,sizeid);
			strcpy(duplicatedbpath,DUPLICATEDB);
			strcat(duplicatedbpath,sizeid);
		}

		char mkcmd[200];
		sprintf(mkcmd, "mkdir -p %s", printdbpath);
		int ress = system(mkcmd);
		sprintf(mkcmd, "mkdir -p %s", duplicatedbpath);
		ress=system(mkcmd);
		if(ress<0)
	    	perror("Error creating folders for databases\n");


		printf("Removing old databases\n");
		//remove databases if exist
		remove_db(duplicatedbpath,dbporiginal[aux],envporiginal[aux]);
		remove_db(printdbpath,dbprinter[aux],envprinter[aux]);

		printf("Initing new database\n");
		init_db(duplicatedbpath,dbporiginal[aux],envporiginal[aux]);

	}


  //initialize analyzis variables
  bzero(incomplete_blocks,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));
  bzero(incomplete_space,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));
  

	//initialize analyzis variables
	bzero(total_blocks,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));
	//identical chunks (that could be eliminated)
	bzero(eq,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));
	//distinct chunks
	bzero(dif,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));
	//distinct chunks with duplicates
	bzero(distinctdup,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));
	//chunks that were appended with zeros due to their size
	//bzero(zeroed_blocks,nr_sizes_proc*(sizeof(uint64_t)));
	//duplicated disk space
	bzero(space,(nr_sizes_proc+nr_sizes_proc_rabin)*(sizeof(uint64_t)));

	//initialize rabin block
    initialize_rabin_polynomial_defaults();
    cur_block=malloc(sizeof(struct rab_block_info *)*nr_sizes_proc_rabin);
	int auxc=0;
	for(auxc=0;auxc<nr_sizes_proc_rabin;auxc++){
	  	  cur_block[auxc]=NULL;
	}	


	//check if it is a folder or device and start processing
	if(devicetype==FOLDER){
		printf("start processing folder %s\n",devicepath);
		search_dir(devicepath);
	}
	else{
		printf("start processing device %s\n",devicepath);
		extract_blocks(devicepath);
	}

	for(aux=0;aux<(nr_sizes_proc+nr_sizes_proc_rabin);aux++){

		if(aux>=nr_sizes_proc)
			fprintf(stderr,"\n\n\nRabin Results for %d\n",sizes_proc_rabin[aux-nr_sizes_proc]);
		else
			fprintf(stderr,"\n\n\nFixed Size Results for %d\n",sizes_proc[aux]);
		fprintf(stderr,"files scanned %llu\n",(unsigned long long int)nfiles);
		fprintf(stderr,"total blocks scanned %llu\n",(unsigned long long int)total_blocks[aux]);
		//fprintf(stderr,"total blocks with zeros appended %llu\n",(unsigned long long int)zeroed_blocks[aux]);
		//blocks without any duplicate are the distinct block minus the distinct blocks with duplicates
		uint64_t zerodups=dif[aux]-distinctdup[aux];
		fprintf(stderr,"blocks without duplicates %llu\n",(unsigned long long int)zerodups);
		fprintf(stderr,"distinct blocks with duplicates %llu\n",(unsigned long long int)distinctdup[aux]);
		fprintf(stderr,"duplicate blocks %llu\n",(unsigned long long int)eq[aux]);
		fprintf(stderr,"space saved %llu Bytes\n",(unsigned long long int)space[aux]);

    fprintf(stderr,"incomplete blocks %llu\n",(unsigned long long int)incomplete_blocks[aux]);
    fprintf(stderr,"incomplete blocks space %llu Bytes\n",(unsigned long long int)incomplete_space[aux]);



		close_db(dbporiginal[aux],envporiginal[aux]);
		//TODO this is not removed to keep the database for dedisgen-utils
		//remove_db(duplicatedbpath,dbporiginal,envporiginal);
	}

	//free memory

  free(incomplete_space);
  free(incomplete_blocks);
	free(total_blocks);
	free(eq);
	free(dif);
	free(distinctdup);
	//free(zeroed_blocks);
	free(space);

	free(dbporiginal);
	free(envporiginal);
	free(dbprinter);
	free(envprinter);

return 0;

}






