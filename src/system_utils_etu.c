/**
 * @file 
 * @brief Implementation by students of usefull function for the system project.
 * @todo Change the SU_removeFile to use exec instead of system.
 */


#include "system_utils.h"

/**
 * @brief Maximum length (in character) for a command line.
 **/
#define SU_MAXLINESIZE (1024*8) 

/********************** File managment ************/

void SU_removeFile(const char * file){
	pid_t pid = fork();
  /* VERY Dirty tmp version */
	if (pid == 0)
	{
		int resSys = execlp("rm", "rm", file, (char*)NULL);
		assert(resSys != -1); // TODO : change with err
	}

	wait(NULL);
}
