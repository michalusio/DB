use std::{iter::FusedIterator, ops::Range, rc::Rc};

use log_err::LogErrResult;

pub trait SplittableByLengthEncoding {
    fn split_by_length_encoding(self) -> SplitByLengthEncoding;
}

impl SplittableByLengthEncoding for Rc<[u8]> {
    fn split_by_length_encoding(self) -> SplitByLengthEncoding
    {
        SplitByLengthEncoding::new(self)
    }
}

#[derive(Clone)]
pub struct SplitByLengthEncoding {
    slice: Rc<[u8]>,
    position: usize
}

impl SplitByLengthEncoding {
    fn new(slice: Rc<[u8]>) -> Self {
        SplitByLengthEncoding {
            slice,
            position: 0
         }
    }
}

impl Iterator for SplitByLengthEncoding
{
    type Item = (Rc<[u8]>, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.slice.len() <= self.position {
            return None;
        }
        let vint_bytes = vint64::decoded_len(self.slice[self.position]);

        let mut vint = &self.slice[self.position..][..vint_bytes];

        self.position += vint_bytes;

        let entry_length: usize = vint64::decode(&mut vint)
            .log_expect("Slice not enough to encode an entry length")
            .try_into()
            .log_unwrap();

        if self.slice.len() < self.position + entry_length {
            panic!("Slice not enough to encode an entry");
        }
        let ret = (self.slice.clone(), Range { start: self.position, end: self.position + entry_length });
        self.position += entry_length;
        Some(ret)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // If there's only one value inside, we yield one slice.
        // If it matches every other element, we yield (n+1)/2 slices (zero-length slices).
        (1, Some((self.slice.len() - self.position).div_ceil(2)))
    }
}

impl FusedIterator for SplitByLengthEncoding {}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use itertools::Itertools;
    use serial_test::parallel;
    use crate::{utils::SplittableByLengthEncoding};

    #[test]
    #[parallel]
    fn test_splitting() {
        let mut data: Vec<u8> = vec![];
        data.extend(vint64::encode(5).as_ref());
        data.extend([1, 2, 3, 4, 5]);
        data.extend(vint64::encode(8).as_ref());
        data.extend([8, 7, 6, 5, 4, 3, 2, 1]);

        let data: Rc<[u8]> = data.into_boxed_slice().into();

        let splitted = data
            .split_by_length_encoding()
            .collect_vec();

        assert_eq!(splitted.len(), 2);
        assert_eq!(splitted[0].0[splitted[0].1.clone()], [1, 2, 3, 4, 5]);
        assert_eq!(splitted[1].0[splitted[1].1.clone()], [8, 7, 6, 5, 4, 3, 2, 1]);
    }
}
