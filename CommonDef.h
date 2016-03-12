#ifndef _COMMON_DEF_H
#define _COMMON_DEF_H
static const size_t MAX_COLUMNS = 512;
static const size_t MAX_ROW_SIZE = 4096;
typedef std::array<const char*, MAX_COLUMNS> Columns;
typedef std::array<char*, MAX_COLUMNS> VColumns;
static char NEWLINE[1] = {'\n'};
#endif

