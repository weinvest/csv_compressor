#ifndef _COLUMN_DECOMPRESSOR_H
#define _COLUMN_DECOMPRESSOR_H
#include <string>
#include <vector>
#include <array>
#include <boost/iostreams/filtering_stream.hpp>
#include "CommonDef.h"
namespace bio = boost::iostreams;
class ColumnDecompressor
{
public:
    ColumnDecompressor(char delimiter, size_t nCacheSize);
    ~ColumnDecompressor();

    void Decompress(const std::string& inputFileName, const std::string& outputFileName);

private:
    struct RowCache
    {
        Columns Fields;
        void WriteOut(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nTotalColumns);
    };
    void WriteOutCache(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nTotalColumns);

    std::vector<RowCache*> mRows;
    const size_t mCacheSize;
    char mDelimiter;
    char mNewLine[1];
    char* mCachePool;
};
#endif

