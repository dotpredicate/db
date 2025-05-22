
// Serialization impl for Client<->Server communication

pub trait Serializable<'a> : Sized {
    fn serialized(&'a self) -> &'a [u8];
}

impl<'a> Serializable<'a> for u32 {
    fn serialized(&'a self) -> &'a [u8] {
        unsafe {
            // Rust dark "unsafe" magic just to be able to view u32 as a byte ptr 
            // (u32::to_le_bytes makes a copy)
            // FIXME: Will this fail on big endian systems?
            std::slice::from_raw_parts(self as *const u32 as *const u8, std::mem::size_of::<u32>())
        }
    }
}

impl<'a> Serializable<'a> for &'a str {
    fn serialized(&'a self) -> &'a [u8] {
        str::as_bytes(self)
    }
}

impl<'a> Serializable<'a> for f64 {
    fn serialized(&'a self) -> &'a [u8] {
        unsafe {
            // Rust dark "unsafe" magic just to be able to view u32 as a byte ptr 
            // (f64::to_le_bytes makes a copy)
            // FIXME: Will this fail on big endian systems?
            std::slice::from_raw_parts(self as *const f64 as *const u8, std::mem::size_of::<f64>())
        }
    }
}

impl<'a> Serializable<'a> for Vec<u8> {
    fn serialized(&'a self) -> &'a [u8] {
        self.as_slice()
    }
}

impl<'a, const N: usize> Serializable<'a> for [u8; N] {
    fn serialized(&'a self) -> &'a [u8] {
        self.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::Serializable;

    #[test]
    fn storable_f64_is_le_bytes() {
        let val: f64 = 3.14159;
        assert_eq!(&val.to_le_bytes(), val.serialized());
    }

    #[test]
    fn storable_u32_is_le_bytes() {
        let val: u32 = 100;
        assert_eq!(&val.to_le_bytes(), val.serialized());
    }

}