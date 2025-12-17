#include "yundb/filter_policy.h"
#include "util/hash.h"


namespace yundb
{

static uint32_t bloomHash(const Slice& key)
{return hash(key.data(), key.size(), 0xfeedbeef);}

class BloomPolicyFilter : public FilterPolicy
{
 public:
  explicit BloomPolicyFilter(uint32_t bits_per_key);
  virtual ~BloomPolicyFilter(){};

  virtual const char* Name() const override
  {return "bloom Filter";}
  // Create filter and append dst
  virtual void createFilter(const Slice* keys,
                            int n, std::string* dst) const override;
  // Return true if the key was 
  // in the list of keys passed to createFilter().
  virtual bool keyMayMatch(const Slice& key, const Slice& filter) const override;
 private:
  uint32_t _bits_per_key;
};

BloomPolicyFilter::BloomPolicyFilter(uint32_t bits_per_key)
      : _bits_per_key(bits_per_key) {}  

void BloomPolicyFilter::createFilter(const Slice* keys,
                                     int n, std::string* dst) const 
{
  CERR_PRINT_WITH_CONDITIONAL("BloomPolicyFilter: None keys", keys == nullptr);
  CERR_PRINT_WITH_CONDITIONAL("BloomPolicyFilter: None dst", dst == nullptr);
  CERR_PRINT_WITH_CONDITIONAL("BloomPolicyFilter: error n value", n <= 0);

  uint32_t m = static_cast<uint32_t>(_bits_per_key * n);
  uint32_t k = static_cast<uint32_t>(_bits_per_key * 0.69); // ln(2) ≈ 0.69

  if (k < 1) k = 1;
  if (k > 30) k = 30;
  if (m < 64) m = 64;

  uint32_t bytes = (m + 7) / 8;
  m = bytes * 8;

  // Expand capacity
  const int initSize = dst->size();
  dst->resize(initSize + bytes, 0);
  // Push k
  dst->push_back(static_cast<char>(k));
  char* array = &(*dst)[initSize];

  // Double-hashing
  for (int i = 0; n > i; i++)
  {
    uint32_t h = bloomHash(keys[i]);
    const uint32_t delte = (h >> 17) | (h << 15);

    for (int j = 0; k > j; j++)
    {
      const uint32_t bitPos = (h % m);
      array[(bitPos / 8)] |= (1 << (bitPos % 8));
      h += delte;
    }
  }
}                                            

bool BloomPolicyFilter::keyMayMatch(const Slice& key, const Slice& filter) const
{
  const char* array = filter.data();
  const uint32_t arrayBytes = filter.size() - 1;
  const uint32_t m = arrayBytes * 8;
  uint32_t k = static_cast<uint32_t>(array[arrayBytes]);

  uint32_t h = bloomHash(key);
  const uint32_t delte = (h >> 17) | (h << 15);

  for (int i = 0; k > i; i++)
  {
    const uint32_t bitPos = (h % m);
    if (!(array[(bitPos / 8)] & (1 << (bitPos % 8))))
      return false;
    h += delte;
  }

  return true;
}

FilterPolicy* BloomPolicyFilter()
{
  static FilterPolicy* policy = nullptr;
  if (policy == nullptr) policy = new class BloomPolicyFilter(10);
  return policy;
}

}