#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/assert.h"

namespace kuzu {
namespace common {

template<typename T>
class MaybeUninit {
    std::aligned_storage_t<sizeof(T), alignof(T)> data;

public:
    T& assumeInit() { return *ptr(); }
    const T& assumeInit() const { return *ptr(); }
    T* ptr() { return reinterpret_cast<T*>(&data); }
    const T* ptr() const { return reinterpret_cast<const T*>(&data); }
};

template<typename T, size_t N>
class StaticVector {
    MaybeUninit<T> items[N];
    size_t len;

    StaticVector(const StaticVector& other) : len(other.len) {
        std::uninitialized_copy(other.begin(), other.end(), begin());
    }

public:
    StaticVector() : len(0){};
    StaticVector(StaticVector&& other) : len(other.len) {
        std::uninitialized_move(other.begin(), other.end(), begin());
        other.len = 0;
    }
    StaticVector copy() const { return *this; }
    StaticVector& operator=(const StaticVector& other) = delete;
    StaticVector& operator=(StaticVector&& other) {
        if (&other != this) {
            clear();
            len = other.len;
            std::uninitialized_move(other.begin(), other.end(), begin());
            other.len = 0;
        }
        return *this;
    }
    ~StaticVector() { clear(); }

    T& operator[](size_t i) {
        KU_ASSERT(i < len);
        return items[i].assumeInit();
    }
    const T& operator[](size_t i) const {
        KU_ASSERT(i < len);
        return items[i].assumeInit();
    }
    void push_back(T elem) {
        KU_ASSERT(len < N);
        new (items[len].ptr()) T(std::move(elem));
        len++;
    }
    T pop_back() {
        KU_ASSERT(len > 0);
        len--;
        return std::move(items[len].assumeInit());
    }
    T* begin() { return items[0].ptr(); }
    const T* begin() const { return items[0].ptr(); }
    T* end() { return items[len].ptr(); }
    const T* end() const { return items[len].ptr(); }

    void clear() {
        std::destroy(begin(), end());
        len = 0;
    }

    bool empty() const { return len == 0; }
    bool full() const { return len == N; }
    size_t size() const { return len; }
};

} // namespace common
} // namespace kuzu
