pub mod capability;
pub mod endpoint;
pub mod health;
pub mod id;
pub mod manifest;
pub mod record;
pub mod state;
pub mod version;

pub use capability::{ServiceCapability, ServicePermissions};
pub use endpoint::{ServiceEndpoint, ServiceProtocol};
pub use health::ServiceHealth;
pub use id::{ServiceId, ServiceInstanceId};
pub use manifest::{
    ServiceManifest, ServiceManifestBuilder, CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION,
};
pub use record::ServiceRecord;
pub use state::ServiceState;
pub use version::ServiceVersion;
