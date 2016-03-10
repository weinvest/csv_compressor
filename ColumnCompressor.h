#ifndef COLUMNCOMPRESSOR_H
#define COLUMNCOMPRESSOR_H
#include <memory>
#include <array>
#include <boost/iostreams/filtering_stream.hpp>
static const size_t MAX_COLUMNS = 512;
namespace bio = boost::iostreams;
class ColumnCompressor
{
public:

    ColumnCompressor(char delimiter, size_t nBlockRows);

    void Compress(const std::string& inputFileName, const std::string& outputFileName);
private:

    bool Next(const char* currentAddress,const char* endAddress, std::array<const char*, MAX_COLUMNS>& spliters, size_t& columns);


    const size_t mBlockRows;
    size_t mTotalRows;
    size_t mTotalColumns;
    size_t mCurrentRowsInBlock;

    enum type
    {
        INT, DOUBLE, STRING
    };

    std::shared_ptr<bio::filtering_ostream> mOutputStream;
    char mDelimiter;
    char mNewLine[1];
};

#endif // COLUMNCOMPRESSOR_H
