#define _GNU_SOURCE
#include <dirent.h>     /* Defines DT_* constants */
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#define handle_error(msg) \
               do { perror(msg); exit(EXIT_FAILURE); } while (0)

struct linux_dirent64 {
    ino64_t        d_ino;    /* 64-bit inode number */
    off64_t        d_off;    /* 64-bit offset to next structure */
    unsigned short d_reclen; /* Size of this dirent */
    unsigned char  d_type;   /* File type */
    char           d_name[]; /* Filename (null-terminated) */
};

#define BUF_SIZE 1024

int main(int argc, char *argv[])
{
    int fd;
    long nread;
    char buf[BUF_SIZE];
    struct linux_dirent64 *d;

    fd = open(argc > 1 ? argv[1] : ".", O_RDONLY | O_DIRECTORY);
    if (fd == -1)
        handle_error("open");

    for (;;) {
        nread = syscall(SYS_getdents64, fd, buf, BUF_SIZE);
        if (nread == -1)
            handle_error("getdents64");

        if (nread == 0)
            break;

        for (long bpos = 0; bpos < nread;) {
            d = (struct linux_dirent64 *) (buf + bpos);
            dprintf(1, "%ld", d->d_ino);
            write(1, "\0\t", 2);
            dprintf(1, "%-10s", (d->d_type == DT_REG) ?  "regular" :
                   (d->d_type == DT_DIR) ?  "directory" :
                   (d->d_type == DT_FIFO) ? "FIFO" :
                   (d->d_type == DT_SOCK) ? "socket" :
                   (d->d_type == DT_LNK) ?  "symlink" :
                   (d->d_type == DT_BLK) ?  "block dev" :
                   (d->d_type == DT_CHR) ?  "char dev" : "???");
            write(1, "\0\t", 2);
            dprintf(1, "%d", d->d_reclen);
            write(1, "\0\t", 2);
            dprintf(1, "%jd", (intmax_t) d->d_off);
            write(1, "\0\t", 2);
            dprintf(1, "%s", d->d_name);
            write(1, "\0\n", 2);
            if (d->d_reclen != 0) {
                bpos += d->d_reclen;
                continue;
            }

            // invalid d_reclen, bruteforcing:
            bpos += sizeof(struct linux_dirent64) + strlen(d->d_name);
            while(bpos < nread) {
                d = (struct linux_dirent64 *) (buf + bpos);
                if (d->d_reclen > BUF_SIZE) {
                    bpos++;
                    continue;
                }
                switch(d->d_type) {
                case DT_REG:
                case DT_DIR:
                case DT_FIFO:
                case DT_SOCK:
                case DT_LNK:
                case DT_BLK:
                case DT_CHR:
                    break;
                default:
                    bpos++;
                    continue;
                }
                break;
            }
        }
    }

    exit(EXIT_SUCCESS);
}