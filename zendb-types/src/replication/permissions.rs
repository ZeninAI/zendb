//! Independent workspace permissions stored with active installations.

use bincode::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
#[repr(u8)]
pub enum Permission {
    ReadData,            // Consume the data from the application tables
    WriteData,           // Write data to application tables
    ManageTables,        // Read-Write permission to manage the tables catalog
    ManageInstallations, // Read-Writer permission to manage the installations catalog
}

impl Permission {
    const fn bit(self) -> u32 {
        1 << self as u8
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode)]
pub struct Permissions(u32);

impl Permissions {
    pub const NONE: Self = Self(0);
    pub const READ_ONLY: Self = Self(Permission::ReadData.bit());
    pub const READ_WRITE: Self = Self(Permission::ReadData.bit() | Permission::WriteData.bit());
    pub const TABLE_CONTROL: Self = Self(Self::READ_WRITE.0 | Permission::ManageTables.bit());
    pub const FULL: Self = Self(Self::TABLE_CONTROL.0 | Permission::ManageInstallations.bit());
    pub const INSTALLATION_CONTROL: Self = Self(Permission::ManageInstallations.bit());

    pub const fn allows(self, permission: Permission) -> bool {
        self.0 & permission.bit() != 0
    }

    pub const fn with(self, permission: Permission) -> Self {
        Self(self.0 | permission.bit())
    }

    pub const fn without(self, permission: Permission) -> Self {
        Self(self.0 & !permission.bit())
    }
}
