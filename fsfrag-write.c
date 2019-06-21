/*******************************************************************************
* Copyright (C) 2019, Jean-Yves VET, jyvet [at] ddn [dot] com                  *
*                                                                              *
* This software is licensed as described in the file LICENCE, which you should *
* have received as part of this distribution. You may opt to use, copy,        *
* modify, merge, publish, distribute and/or sell copies of the Software, and   *
* permit persons to whom the Software is furnished to do so, under the terms   *
* of the LICENCE file.                                                         *
*                                                                              *
* This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY    *
* KIND, either express or implied.                                             *
*******************************************************************************/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <argp.h>
#include <unistd.h>
#include <assert.h>
#include <sys/time.h>
#include <mpi.h>

#define FTYPE MPI_File

#define MPI_CHECK(MPI_STATUS, MSG)                                       \
do {                                                                     \
    char res_str[MPI_MAX_ERROR_STRING];                                  \
    int res_len;                                                         \
                                                                         \
    if (MPI_STATUS != MPI_SUCCESS) {                                     \
        MPI_Error_string(MPI_STATUS, res_str, &res_len);                 \
        fprintf(stdout, "ERROR: %s, MPI %s, (%s:%d)\n",                  \
                MSG, res_str, __FILE__, __LINE__);                       \
        MPI_Abort(MPI_COMM_WORLD, -1);                                   \
    }                                                                    \
} while(0)

/* Expand Macro values to string */
#define STR_VALUE(var)   #var
#define STR(var)         STR_VALUE(var)


#define DEFAULT_FILES_PER_PROC       100
#define DEFAULT_MAX_OPEN_FILES       200
#define DEFAULT_BLOCK_SIZE        131072  /* 128 KB */
#define DEFAULT_FILE_SIZE       16777216  /*  16 MB */

typedef struct timer_s
{
    int64_t          elapsed;      /*  Elapsed time (in miliseconds)  */
    struct timeval   start;        /*  Start time                     */
} ptimer_t;

static void
timer_start(ptimer_t* timer)
{
    gettimeofday(&timer->start, 0);
}

static void
timer_stop(ptimer_t* timer)
{
    struct timeval stop;
    struct timeval start = timer->start;
    gettimeofday(&stop, 0);
    timer->elapsed += ((stop.tv_sec - start.tv_sec) * 1000L)
                    + ((stop.tv_usec - start.tv_usec) / 1000L);
}

static char *buffer;

ptimer_t timer, timer_wo_open;
static char dest_path[PATH_MAX + 1];
static uint64_t nb_files = DEFAULT_FILES_PER_PROC;
static uint64_t file_size = DEFAULT_FILE_SIZE;
static uint64_t block_size = DEFAULT_BLOCK_SIZE;
static uint64_t max_open_files = DEFAULT_MAX_OPEN_FILES;
static uint64_t timestamp;
static int rank, size;
const char *argp_program_version     = "fsfrag-write 0.1";
const char *argp_program_bug_address = "<jyvet [at] ddn.com>";


/* Program documentation */
static char doc[] = "Each process creates <nfiles> files in the <dir_path> "
                    "directory and appends <bsize> Bytes round robbin in each "
                    "file until reaching <fsize> total size per file. "
                    "Batches are used to avoid hitting the limit of files "
                    "opened simultaneously. It accepts the following arguments:";

/* A description of the arguments we accept */
static char args_doc[] = "<dir_path>";

/* The options we understand */
static struct argp_option options[] = {
    {"bsize",      'b', "<bytes>",   0, "Size in bytes of each block [default: "
                                         STR(DEFAULT_BLOCK_SIZE)" B]"},
    {"nfiles",     'n', "<N>",       0, "Files per process [default: "
                                         STR(DEFAULT_FILES_PER_PROC)"]"},
    {"ofiles",     'o', "<N>",       0, "Max simultaneous opened files [default: "
                                         STR(DEFAULT_MAX_OPEN_FILES)"]"},
    {"fsize",      'f', "<bytes>",   0, "Size in bytes of each file [default: "
                                         STR(DEFAULT_FILE_SIZE)" B]"},
    { 0 }
};

/* Parse a single option */
static error_t
parse_opt(int key, char *arg, struct argp_state *state) {
    switch (key) {
        case 'b':
            block_size = (uint64_t)atol(arg);
            assert(block_size > 0);
            break;

        case 'n':
            nb_files = (uint64_t)atol(arg);
            assert(nb_files > 0);
            break;

        case 'o':
            max_open_files = (uint64_t)atol(arg);
            assert(max_open_files > 0);
            break;

        case 'f':
            file_size = (uint64_t)atol(arg);
            assert(file_size > 0);
            break;

        case ARGP_KEY_ARG:
            if (state->arg_num == 0)
                strncpy(dest_path, arg, PATH_MAX);
            break;

        case ARGP_KEY_END:
            if (state->arg_num != 1)
                argp_usage (state);
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }

    return 0;
}

/* Argp parser */
static struct argp argp = { options, parse_opt, args_doc, doc };


void
fill_buffer(char *buffer, const uint64_t size)
{
    memset(buffer, '@', size);
}

static FTYPE*
create_files(const uint32_t rank,
             const uint64_t nb_files,
             const uint64_t first_id)
{
    FTYPE *files = malloc(nb_files * sizeof(FTYPE));
    assert(files != NULL);

    char buff[PATH_MAX + 1];
    for (uint64_t i = 0; i < nb_files; i++)
    {
        snprintf(buff, PATH_MAX, "%s/test-%lu-%d.%lu",
                                 dest_path, timestamp, rank, first_id + i);

        MPI_CHECK(MPI_File_open(MPI_COMM_SELF, buff, MPI_MODE_CREATE |
                                MPI_MODE_WRONLY | MPI_MODE_APPEND | O_TRUNC,
                                MPI_INFO_NULL, &files[i]), "cannot open file");
    }

    return files;
}

static void
close_files(FTYPE *files, const uint64_t nb_files)
{
    for (uint64_t i = 0; i < nb_files; i++)
    {
        MPI_CHECK(MPI_File_close(&files[i]), "cannot close file");
    }
}

static void
write_files(FTYPE *files, const uint64_t nb_files, char *buffer)
{
    const uint64_t full_blocks = file_size / block_size;
    for (uint64_t i = 0; i < full_blocks; i++)
    {
        void *ptr = &buffer[i * block_size];

        for (uint64_t f = 0; f < nb_files; f++)
        {
            MPI_Status status;
            MPI_CHECK(MPI_File_write(files[f], ptr, block_size, MPI_BYTE,
            &status), "Cannot write block");
        }
    }

    const uint64_t last_block_size = file_size % block_size;
    if (last_block_size != 0)
    {
        void *ptr = &buffer[full_blocks * block_size];

        for (uint64_t f = 0; f < nb_files; f++)
        {
            MPI_Status status;
            MPI_CHECK(MPI_File_write(files[f], ptr, last_block_size, MPI_BYTE,
            &status), "Cannot write last block");
        }
    }
}

void
start_batch(const uint32_t rank,
            const uint64_t nb_files,
            const uint64_t first_id)
{
    FTYPE *files = create_files(rank, nb_files, first_id);

    MPI_Barrier(MPI_COMM_WORLD);
    timer_start(&timer_wo_open);
    write_files(files, nb_files, buffer);

    close_files(files, nb_files);

    MPI_Barrier(MPI_COMM_WORLD);
    timer_stop(&timer_wo_open);

    free(files);
}

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0)
    {
        argp_parse (&argp, argc, argv, 0, 0, NULL);

        struct timeval tv;
        gettimeofday(&tv, NULL);
        timestamp = tv.tv_sec;

        printf("Directory path: %s\n", dest_path);
        printf("NB files: %ld (%ld files per process)\n",
               nb_files * size, nb_files);
        printf("File size: %ld\n", file_size);
        printf("Block size: %ld\n", block_size);
    }

    MPI_Bcast(&nb_files, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(&file_size, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(&block_size, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(&timestamp, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(&max_open_files, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(&dest_path, PATH_MAX + 1, MPI_BYTE, 0, MPI_COMM_WORLD);

    buffer = malloc(file_size);
    fill_buffer(buffer, file_size);

    const uint64_t nb_complete_batches = nb_files / max_open_files;
    const uint64_t last_batch_files = nb_files % max_open_files;
    const uint64_t nb_batches = nb_complete_batches + (last_batch_files > 0);

    MPI_Barrier(MPI_COMM_WORLD);


    if (rank == 0)
        printf("Writing now to files...\n");

    timer_start(&timer);

    for (uint64_t i = 0; i < nb_complete_batches; i++)
    {
        if (rank == 0)
            printf("  - batch %lu/%lu (%lu files per process)\n",
                   i + 1, nb_batches, max_open_files);

        start_batch(rank, max_open_files, max_open_files * i);
    }

    if (last_batch_files != 0)
    {
        if (rank == 0)
            printf("  - batch %lu/%lu (%lu files per process)\n",
                   nb_batches, nb_batches, last_batch_files);

        start_batch(rank, last_batch_files, max_open_files * nb_batches);
    }

    timer_stop(&timer);

    if (rank == 0)
    {
        uint64_t total_bytes = (nb_files * file_size * size);
        uint64_t bw = 0;

        if (timer_wo_open.elapsed != 0)
            bw = total_bytes / (timer_wo_open.elapsed * 1000L);

        printf("Total time: %lu s\n", (timer.elapsed / 1000L));
        printf("Open time: %lu us\n", (timer.elapsed - timer_wo_open.elapsed));
        printf("BW: %lu MiB/s\n", bw);
    }

    MPI_Finalize();

    return 0;
}

