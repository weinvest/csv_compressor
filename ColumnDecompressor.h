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
        VColumns Fields;
        std::array<char, 4096> Data;

        RowCache();
        void WriteOut(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nTotalColumns);
    };
    void WriteOutCache(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nTotalColumns);
    RowCache& EnsureRowCache(size_t nRow);
    void Append2Rows(std::vector<boost::iterator_range<std::string::iterator>>& column, size_t nColumnIndex);

    std::vector<std::shared_ptr<RowCache>> mRows;
    const size_t mCacheSize;
    char mDelimiter;
};
#endif

