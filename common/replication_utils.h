//
// Created by jrsoares on 28-03-2025.
//

#include <type_traits>

namespace slog {

template <typename N>
static inline std::enable_if_t<std::is_integral_v<N>, N> more_than_half(N v) {
  return (v > 2) ? (v / 2) + 1 : v;
}

}
