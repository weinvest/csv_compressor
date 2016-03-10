#include "ColumnCompressor.h"

ColumnCompressor::ColumnCompressor(char delimiter, size_t nBlockRows)
    :mBlockRows(nBlockRows)
    ,mTotalRows(0)
    ,mTotalColumns(0)
    ,mCurrentRowsInBlock(0)
    ,mDelimiter(delimiter)
{
    mNewLine[0] = '\n';
}

void ColumnCompressor::Compress(const std::string& inputFileName, const std::string& outputFileName)
{
    mOutputStream.reset(new bio::filtering_ostream());
}

bool ColumnCompressor::Next(const char* currentAddress, const char* endAddress, std::array<const char*, MAX_COLUMNS>& spliters, size_t& columns)
{
    columns = 0;
    spliters[0] = ++currentAddress;
    while(currentAddress != endAddress && *currentAddress != mNewLine[0])
    {
        if(*currentAddress == mDelimiter)
        {
            spliters[++columns] = currentAddress + 1;
        }
        ++currentAddress;
    }

    return currentAddress != endAddress;
}
