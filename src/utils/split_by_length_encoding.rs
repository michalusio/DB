use std::iter::FusedIterator;

pub trait SplittableByLengthEncoding {
    fn split_by_length_encoding(&self) -> SplitByLengthEncoding<'_>;
}

impl SplittableByLengthEncoding for [u8] {
    #[inline]
    fn split_by_length_encoding(&self) -> SplitByLengthEncoding<'_>
    {
        SplitByLengthEncoding::new(self)
    }
}

#[derive(Clone)]
pub struct SplitByLengthEncoding<'a> {
    slice: &'a [u8]
}

impl<'a> SplitByLengthEncoding<'a> {
    fn new(slice: &'a[u8]) -> Self {
        SplitByLengthEncoding {
            slice
         }
    }
}

impl<'a> Iterator for SplitByLengthEncoding<'a>
{
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.slice.is_empty() {
            return None;
        }
        
        let entry_length: usize = vint64::decode(&mut self.slice).expect("Slice not enough to encode an entry length").try_into().unwrap();

        if self.slice.len() < entry_length {
            panic!("Slice not enough to encode an entry");
        }

        let ret = &self.slice[0..entry_length];
        self.slice = &self.slice[entry_length..];
        Some(ret)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // If there's only one value inside, we yield one slice.
        // If it matches every other element, we yield (n+1)/2 slices (zero-length slices).
        (1, Some((self.slice.len() + 1) / 2))
    }
}

impl FusedIterator for SplitByLengthEncoding<'_> {}