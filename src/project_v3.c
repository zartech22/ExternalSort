/**
 * @file
 * @brief Implementation of the V0 of the system project.
 * @warning You should not modifie the current file.
 */

#include <pthread.h>

#include "project_v3.h"

struct sortFileArguments
{
	char const * filenameSource;
	char const * filenameSorted;
	unsigned long nb;
};

struct mergeFileArguments
{
	unsigned int id;

	unsigned long start;
	unsigned long end;

	const char **SortedFilenames;
};

typedef struct sortFileArguments sortFileArguments;
typedef struct mergeFileArguments mergeFileArguments;

 /**
  * @brief Maximum length (in character) for a file name.
  **/
#define PROJECT_FILENAME_MAX_SIZE 1024

  /**
   * @brief Type of the sort algorithm used in this version.
   **/
   //#define SORTALGO(nb_elem, values) SU_ISort(nb_elem, values)
   //#define SORTALGO(nb_elem, values) SU_HSort(nb_elem, values)
#define SORTALGO(nb_elem, values) SU_QSort(nb_elem, values)

/**********************************/

void projectV3(const char * i_file, const char * o_file, unsigned long nb_split)
{

	/* Get number of line to sort */
	int nb_print = 0;
	unsigned long nb_lines = SU_getFileNbLine(i_file);
	unsigned long nb_lines_per_files = nb_lines / (unsigned long)nb_split;

	fprintf(stderr,
		"Projet version 3 version with %lu split of %lu lines\n",
		nb_split,
		nb_lines_per_files);

	/* 0 - Deal with case nb_split = 1 */
	if (nb_split < 2)
	{
		int * values = NULL;
		unsigned long nb_elem = SU_loadFile(i_file, &values);

		SORTALGO(nb_elem, values);

		SU_saveFile(o_file, nb_elem, values);

		free(values);
		return;
	}

	/* 1 - Spit the source file */

	/* 1.1 - Create a vector of target filenames for the split */
	char ** filenames = (char**)malloc(sizeof(char*) * (size_t)nb_split);
	char ** filenames_sort = (char**)malloc(sizeof(char*) * (size_t)nb_split);

	unsigned long cpt = 0;

	for (cpt = 0; cpt < nb_split; ++cpt)
	{
		filenames[cpt] = (char *)malloc(sizeof(char) * PROJECT_FILENAME_MAX_SIZE);

		nb_print = snprintf(filenames[cpt],
			PROJECT_FILENAME_MAX_SIZE,
			"/tmp/tmp_split_%d_%lu.txt", getpid(), cpt);

		if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
			err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);

		filenames_sort[cpt] = (char *)malloc(sizeof(char) * PROJECT_FILENAME_MAX_SIZE);

		nb_print = snprintf(filenames_sort[cpt],
			PROJECT_FILENAME_MAX_SIZE,
			"/tmp/tmp_split_%d_%lu.sort.txt", getpid(), cpt);

		if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
			err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);
	}

	/* 1.2 - Split the source file */
	SU_splitFile2(i_file,
		nb_lines_per_files,
		nb_split,
		(const char **)filenames
	);

	/* 2 - Sort each file */
	projectV3_sortFiles(nb_split, (const char **)filenames, (const char **)filenames_sort);

	/* 3 - Merge (two by two) */
	projectV3_combMerge(nb_split, (const char **)filenames_sort, (const char *)o_file);

	/* 4 - Clear */
	for (cpt = 0; cpt < nb_split; ++cpt)
	{
		free(filenames[cpt]); // not needed :  clear in sort
		free(filenames_sort[cpt]);
	}

	free(filenames);
	free(filenames_sort);
}

void projectV3_sortFiles(unsigned long nb_split, const char ** filenames, const char ** filenames_sort)
{
	pthread_t *threadsId = (pthread_t*)malloc(sizeof(pthread_t) * nb_split);
	sortFileArguments *arguments = (sortFileArguments*)malloc(sizeof(sortFileArguments) * nb_split);

	unsigned long cpt = 0;

	for (cpt = 0; cpt < nb_split; ++cpt)
	{
		arguments[cpt].filenameSource = filenames[cpt];
		arguments[cpt].filenameSorted = filenames_sort[cpt];
		arguments[cpt].nb = cpt;

		pthread_create(&threadsId[cpt], NULL, &projectV3_sortSingleFile, (void*)(&arguments[cpt]));
	}

	for (cpt = 0; cpt < nb_split; ++cpt)
	{
		pthread_join(threadsId[cpt], NULL);
	}

	free(arguments);
}

void* projectV3_sortSingleFile(void *arg)
{
	const sortFileArguments * const arguments = (sortFileArguments*)arg;

	int * values = NULL;

	unsigned long nb_elem = SU_loadFile(arguments->filenameSource, &values);
	SU_removeFile(arguments->filenameSource);

	fprintf(stderr, "Inner sort %lu: Array of %lu elem by %d\n", arguments->nb, nb_elem, getpid());

	SORTALGO(nb_elem, values);

	SU_saveFile(arguments->filenameSorted, nb_elem, values);
	free(values);

	return NULL;
}

void projectV3_combMerge(unsigned long nb_split, const char ** filenames_sort, const char * o_file)
{
	pthread_t stack1, stack2;
	mergeFileArguments stack1Args, stack2Args;

	unsigned int id = 0;

	int nb_print;
	char previous_name[PROJECT_FILENAME_MAX_SIZE];
	char current_name[PROJECT_FILENAME_MAX_SIZE];


	stack1Args.id = id++;
	stack1Args.start = 0;
	stack1Args.end = (nb_split / 2) + 1;
	stack1Args.SortedFilenames = filenames_sort;

	stack2Args.id = id++;
	stack2Args.start = (nb_split / 2) + 1;
	stack2Args.end = nb_split;
	stack2Args.SortedFilenames = filenames_sort;

	pthread_create(&stack1, NULL, &projectV3_comboMergeStack, (void*)(&stack1Args));
	pthread_create(&stack2, NULL, &projectV3_comboMergeStack, (void*)(&stack2Args));

	pthread_join(stack1, NULL);
	pthread_join(stack2, NULL);

	nb_print = snprintf(previous_name,
		PROJECT_FILENAME_MAX_SIZE,
		"/tmp/final_split_%u_merge.txt", stack1Args.id);

	if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
		err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);

	nb_print = snprintf(current_name,
		PROJECT_FILENAME_MAX_SIZE,
		"/tmp/final_split_%u_merge.txt", stack2Args.id);

	if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
		err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);

	/* Last merge */
	fprintf(stderr, "Final merge sort : %s + %s -> %s \n",
		previous_name,
		current_name,
		o_file);

	SU_mergeSortedFiles(previous_name,
		current_name,
		o_file);

	SU_removeFile(previous_name);
	SU_removeFile(current_name);
}

void * projectV3_comboMergeStack(void * args)
{
	const mergeFileArguments * const arguments = (mergeFileArguments*)args;
	int nb_print;

	char previous_name[PROJECT_FILENAME_MAX_SIZE];

	nb_print = snprintf(previous_name,
		PROJECT_FILENAME_MAX_SIZE,
		"%s", arguments->SortedFilenames[arguments->start]);

	if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
		err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);

	char current_name[PROJECT_FILENAME_MAX_SIZE];

	nb_print = snprintf(current_name,
		PROJECT_FILENAME_MAX_SIZE,
		"/tmp/tmp_split_%u_merge_%d.txt", arguments->id, 0);

	if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
	{
		err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);
	}

	
	for (unsigned long cpt = arguments->start + 1; cpt < arguments->end - 1; ++cpt)
	{
		fprintf(stderr, "Merge sort %lu : %s + %s -> %s \n",
			cpt,
			previous_name,
			arguments->SortedFilenames[cpt],
			current_name);

		SU_mergeSortedFiles(previous_name,
			arguments->SortedFilenames[cpt],
			current_name);

		SU_removeFile(previous_name);
		SU_removeFile(arguments->SortedFilenames[cpt]);

		nb_print = snprintf(previous_name,
			PROJECT_FILENAME_MAX_SIZE,
			"%s", current_name);

		if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
			err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);

		nb_print = snprintf(current_name,
			PROJECT_FILENAME_MAX_SIZE,
			"/tmp/tmp_split_%u_merge_%lu.txt", arguments->id, cpt);

		if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
			err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);
	}

	nb_print = snprintf(current_name,
		PROJECT_FILENAME_MAX_SIZE,
		"/tmp/final_split_%u_merge.txt", arguments->id);

	if (nb_print >= PROJECT_FILENAME_MAX_SIZE)
		err(1, "Out of buffer (%s:%d)", __FILE__, __LINE__);

	/* Last merge */
	fprintf(stderr, "Last merge sort : %s + %s -> %s \n",
		previous_name,
		arguments->SortedFilenames[arguments->end - 1],
		current_name);

	SU_mergeSortedFiles(previous_name,
		arguments->SortedFilenames[arguments->end - 1],
		current_name);

	SU_removeFile(previous_name);
	SU_removeFile(arguments->SortedFilenames[arguments->end - 1]);

	return NULL;
}
