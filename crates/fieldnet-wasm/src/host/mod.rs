pub mod abi;
pub mod context;
pub mod errors;
pub mod events;
pub mod network;
pub mod permissions;
pub mod services;
pub mod storage;

use std::time::Instant;
use tracing::{info, warn};
use wasmtime::{Caller, Linker, Memory};

use crate::execution::store::StoreData;
use crate::host::abi::{
    EventPublishRequest, EventPublishResponse, HostOperation, NetworkSendRequest,
    ServiceDescribeRequest, ServiceResolveRequest, StorageDeleteRequest, StorageDeleteResponse,
    StorageGetRequest, StorageGetResponse, StorageHasRequest, StorageHasResponse,
    StoragePutRequest, StoragePutResponse, FIELDNET_HOST_API_VERSION, MAX_REQUEST_PAYLOAD_BYTES,
};
use crate::host::context::HostContext;
use crate::host::errors::{GuestErrorCode, HostError};

/// Reads a contiguous byte slice from guest linear memory with bounds checking.
pub fn read_guest_memory(
    caller: &Caller<'_, StoreData>,
    memory: &Memory,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>, GuestErrorCode> {
    if ptr < 0 || len < 0 {
        return Err(GuestErrorCode::InvalidRequest);
    }
    if len as usize > MAX_REQUEST_PAYLOAD_BYTES {
        return Err(GuestErrorCode::InvalidRequest);
    }
    let start = ptr as usize;
    let end = match start.checked_add(len as usize) {
        Some(e) => e,
        None => return Err(GuestErrorCode::InvalidRequest),
    };
    let mem_data = memory.data(caller);
    if end > mem_data.len() {
        return Err(GuestErrorCode::InvalidRequest);
    }
    Ok(mem_data[start..end].to_vec())
}

/// Writes a contiguous byte slice into guest linear memory with bounds and capacity checking.
pub fn write_guest_memory(
    caller: &mut Caller<'_, StoreData>,
    memory: &Memory,
    ptr: i32,
    max_len: i32,
    data: &[u8],
) -> Result<i32, GuestErrorCode> {
    if ptr < 0 || max_len < 0 {
        return Err(GuestErrorCode::InvalidRequest);
    }
    let dest_start = ptr as usize;
    let max_size = max_len as usize;
    if data.len() > max_size {
        return Err(GuestErrorCode::BufferTooSmall);
    }
    let mem_len = memory.data(&*caller).len();
    let dest_end = match dest_start.checked_add(data.len()) {
        Some(e) => e,
        None => return Err(GuestErrorCode::InvalidRequest),
    };
    if dest_end > mem_len {
        return Err(GuestErrorCode::InvalidRequest);
    }
    memory
        .write(caller, dest_start, data)
        .map_err(|_| GuestErrorCode::InvalidRequest)?;
    Ok(data.len() as i32)
}

/// Registers the FIELDNET Environment API host functions into the Wasmtime Linker.
pub fn register_host_functions(linker: &mut Linker<StoreData>) -> Result<(), wasmtime::Error> {
    for &module_name in &["fieldnet_v1", "fieldnet"] {
        // 1. Host API Version Discovery
        linker.func_wrap(module_name, "host_api_version", || -> i32 {
            FIELDNET_HOST_API_VERSION
        })?;

        // 2. Unified host_call dispatcher
        linker.func_wrap(
            module_name,
            "host_call",
            |mut caller: Caller<'_, StoreData>,
             op_code: i32,
             req_ptr: i32,
             req_len: i32,
             resp_ptr: i32,
             resp_max_len: i32|
             -> i32 {
                handle_host_call(
                    &mut caller,
                    op_code,
                    req_ptr,
                    req_len,
                    resp_ptr,
                    resp_max_len,
                )
            },
        )?;

        // 3. Typed Host Function: storage_put(data_ptr, data_len, out_cid_ptr, out_cid_max_len) -> i32
        linker.func_wrap(
            module_name,
            "storage_put",
            |mut caller: Caller<'_, StoreData>,
             data_ptr: i32,
             data_len: i32,
             out_cid_ptr: i32,
             out_cid_max_len: i32|
             -> i32 {
                handle_storage_put_raw(
                    &mut caller,
                    data_ptr,
                    data_len,
                    out_cid_ptr,
                    out_cid_max_len,
                )
            },
        )?;

        // 4. Typed Host Function: storage_get(cid_ptr, cid_len, out_data_ptr, out_data_max_len) -> i32
        linker.func_wrap(
            module_name,
            "storage_get",
            |mut caller: Caller<'_, StoreData>,
             cid_ptr: i32,
             cid_len: i32,
             out_data_ptr: i32,
             out_data_max_len: i32|
             -> i32 {
                handle_storage_get_raw(
                    &mut caller,
                    cid_ptr,
                    cid_len,
                    out_data_ptr,
                    out_data_max_len,
                )
            },
        )?;

        // 5. Typed Host Function: storage_has(cid_ptr, cid_len) -> i32 (1 = true, 0 = false, <0 = error)
        linker.func_wrap(
            module_name,
            "storage_has",
            |mut caller: Caller<'_, StoreData>, cid_ptr: i32, cid_len: i32| -> i32 {
                handle_storage_has_raw(&mut caller, cid_ptr, cid_len)
            },
        )?;

        // 6. Typed Host Function: storage_delete(cid_ptr, cid_len) -> i32 (1 = deleted, 0 = not found, <0 = error)
        linker.func_wrap(
            module_name,
            "storage_delete",
            |mut caller: Caller<'_, StoreData>, cid_ptr: i32, cid_len: i32| -> i32 {
                handle_storage_delete_raw(&mut caller, cid_ptr, cid_len)
            },
        )?;

        // 7. Typed Host Function: service_resolve(query_json_ptr, query_len, out_desc_ptr, out_desc_max_len) -> i32
        linker.func_wrap(
            module_name,
            "service_resolve",
            |mut caller: Caller<'_, StoreData>,
             query_ptr: i32,
             query_len: i32,
             out_desc_ptr: i32,
             out_desc_max_len: i32|
             -> i32 {
                handle_service_resolve_raw(
                    &mut caller,
                    query_ptr,
                    query_len,
                    out_desc_ptr,
                    out_desc_max_len,
                )
            },
        )?;

        // 8. Typed Host Function: service_describe(svc_id_ptr, svc_id_len, out_desc_ptr, out_desc_max_len) -> i32
        linker.func_wrap(
            module_name,
            "service_describe",
            |mut caller: Caller<'_, StoreData>,
             svc_id_ptr: i32,
             svc_id_len: i32,
             out_desc_ptr: i32,
             out_desc_max_len: i32|
             -> i32 {
                handle_service_describe_raw(
                    &mut caller,
                    svc_id_ptr,
                    svc_id_len,
                    out_desc_ptr,
                    out_desc_max_len,
                )
            },
        )?;

        // 9. Typed Host Function: event_publish(topic_ptr, topic_len, payload_ptr, payload_len) -> i32
        linker.func_wrap(
            module_name,
            "event_publish",
            |mut caller: Caller<'_, StoreData>,
             topic_ptr: i32,
             topic_len: i32,
             payload_ptr: i32,
             payload_len: i32|
             -> i32 {
                handle_event_publish_raw(
                    &mut caller,
                    topic_ptr,
                    topic_len,
                    payload_ptr,
                    payload_len,
                )
            },
        )?;

        // 10. Typed Host Function: network_send(dest_ptr, dest_len, payload_ptr, payload_len) -> i32
        linker.func_wrap(
            module_name,
            "network_send",
            |mut caller: Caller<'_, StoreData>,
             dest_ptr: i32,
             dest_len: i32,
             payload_ptr: i32,
             payload_len: i32|
             -> i32 {
                handle_network_send_raw(&mut caller, dest_ptr, dest_len, payload_ptr, payload_len)
            },
        )?;
    }

    Ok(())
}

fn dispatch_host_operation(
    host_ctx: &HostContext,
    op: HostOperation,
    req_bytes: &[u8],
) -> Result<Vec<u8>, HostError> {
    match op {
        HostOperation::StoragePut => {
            let req: StoragePutRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid StoragePutRequest: {}", e))
            })?;
            let cid = host_ctx.storage.put(&host_ctx.capabilities, &req.data)?;
            let resp = StoragePutResponse {
                content_id: cid.to_hex(),
                size_bytes: req.data.len() as u64,
            };
            serde_json::to_vec(&resp).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::StorageGet => {
            let req: StorageGetRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid StorageGetRequest: {}", e))
            })?;
            let data = host_ctx
                .storage
                .get(&host_ctx.capabilities, &req.content_id)?;
            let resp = StorageGetResponse {
                content_id: req.content_id,
                data,
            };
            serde_json::to_vec(&resp).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::StorageHas => {
            let req: StorageHasRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid StorageHasRequest: {}", e))
            })?;
            let exists = host_ctx
                .storage
                .has(&host_ctx.capabilities, &req.content_id)?;
            let resp = StorageHasResponse {
                content_id: req.content_id,
                exists,
            };
            serde_json::to_vec(&resp).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::StorageDelete => {
            let req: StorageDeleteRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid StorageDeleteRequest: {}", e))
            })?;
            let deleted = host_ctx
                .storage
                .delete(&host_ctx.capabilities, &req.content_id)?;
            let resp = StorageDeleteResponse {
                content_id: req.content_id,
                deleted,
            };
            serde_json::to_vec(&resp).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::ServiceResolve => {
            let services = host_ctx.services.as_ref().ok_or_else(|| {
                HostError::service_unavailable("Service resolver subsystem not configured")
            })?;
            let req: ServiceResolveRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid ServiceResolveRequest: {}", e))
            })?;
            let desc = services.resolve(&host_ctx.capabilities, &req)?;
            serde_json::to_vec(&desc).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::ServiceDescribe => {
            let services = host_ctx.services.as_ref().ok_or_else(|| {
                HostError::service_unavailable("Service resolver subsystem not configured")
            })?;
            let req: ServiceDescribeRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid ServiceDescribeRequest: {}", e))
            })?;
            let desc = services.describe(&host_ctx.capabilities, &req.service_id)?;
            serde_json::to_vec(&desc).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::EventPublish => {
            let req: EventPublishRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid EventPublishRequest: {}", e))
            })?;
            let topic =
                host_ctx
                    .events
                    .publish(&host_ctx.capabilities, &req.event_type, &req.payload)?;
            let resp = EventPublishResponse {
                canonical_topic: topic,
                published: true,
            };
            serde_json::to_vec(&resp).map_err(|e| HostError::internal(e.to_string()))
        }
        HostOperation::NetworkSend => {
            let network = host_ctx.network.as_ref().ok_or_else(|| {
                HostError::network_unavailable("Network subsystem not configured")
            })?;
            let req: NetworkSendRequest = serde_json::from_slice(req_bytes).map_err(|e| {
                HostError::invalid_request(format!("Invalid NetworkSendRequest: {}", e))
            })?;
            let resp = network.send(&host_ctx.capabilities, &req)?;
            serde_json::to_vec(&resp).map_err(|e| HostError::internal(e.to_string()))
        }
    }
}

fn handle_host_call(
    caller: &mut Caller<'_, StoreData>,
    op_code: i32,
    req_ptr: i32,
    req_len: i32,
    resp_ptr: i32,
    resp_max_len: i32,
) -> i32 {
    let start = Instant::now();
    let app_id = caller.data().application_id.clone();
    let exec_id = caller.data().execution_id.clone();

    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };

    let op = match HostOperation::from_i32(op_code) {
        Some(o) => o,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };

    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };

    // Read request bytes from guest memory
    let req_bytes = match read_guest_memory(caller, &memory, req_ptr, req_len) {
        Ok(b) => b,
        Err(err) => return err.as_i32(),
    };

    let result = dispatch_host_operation(&host_ctx, op, &req_bytes);

    let (return_code, success, error_cat) = match result {
        Ok(resp_bytes) => {
            match write_guest_memory(caller, &memory, resp_ptr, resp_max_len, &resp_bytes) {
                Ok(bytes_written) => (bytes_written, true, None),
                Err(err) => (err.as_i32(), false, Some(err.as_str())),
            }
        }
        Err(host_err) => {
            let code = host_err.code();
            warn!(
                application_id = %app_id,
                execution_id = %exec_id,
                operation = op.as_str(),
                error = %host_err,
                "Host call failed"
            );
            (code.as_i32(), false, Some(code.as_str()))
        }
    };

    info!(
        application_id = %app_id,
        execution_id = %exec_id,
        operation = op.as_str(),
        success = success,
        duration_us = start.elapsed().as_micros(),
        payload_size = req_len,
        error_category = error_cat,
        "Host call completed"
    );

    return_code
}

fn handle_storage_put_raw(
    caller: &mut Caller<'_, StoreData>,
    data_ptr: i32,
    data_len: i32,
    out_cid_ptr: i32,
    out_cid_max_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let data = match read_guest_memory(caller, &memory, data_ptr, data_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };

    match host_ctx.storage.put(&host_ctx.capabilities, &data) {
        Ok(cid) => {
            let cid_bytes = cid.to_hex().into_bytes();
            match write_guest_memory(caller, &memory, out_cid_ptr, out_cid_max_len, &cid_bytes) {
                Ok(written) => written,
                Err(e) => e.as_i32(),
            }
        }
        Err(e) => e.code().as_i32(),
    }
}

fn handle_storage_get_raw(
    caller: &mut Caller<'_, StoreData>,
    cid_ptr: i32,
    cid_len: i32,
    out_data_ptr: i32,
    out_data_max_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let cid_bytes = match read_guest_memory(caller, &memory, cid_ptr, cid_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let cid_str = match std::str::from_utf8(&cid_bytes) {
        Ok(s) => s,
        Err(_) => return GuestErrorCode::InvalidId.as_i32(),
    };

    match host_ctx.storage.get(&host_ctx.capabilities, cid_str) {
        Ok(data) => {
            match write_guest_memory(caller, &memory, out_data_ptr, out_data_max_len, &data) {
                Ok(written) => written,
                Err(e) => e.as_i32(),
            }
        }
        Err(e) => e.code().as_i32(),
    }
}

fn handle_storage_has_raw(caller: &mut Caller<'_, StoreData>, cid_ptr: i32, cid_len: i32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let cid_bytes = match read_guest_memory(caller, &memory, cid_ptr, cid_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let cid_str = match std::str::from_utf8(&cid_bytes) {
        Ok(s) => s,
        Err(_) => return GuestErrorCode::InvalidId.as_i32(),
    };

    match host_ctx.storage.has(&host_ctx.capabilities, cid_str) {
        Ok(exists) => {
            if exists {
                1
            } else {
                0
            }
        }
        Err(e) => e.code().as_i32(),
    }
}

fn handle_storage_delete_raw(
    caller: &mut Caller<'_, StoreData>,
    cid_ptr: i32,
    cid_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let cid_bytes = match read_guest_memory(caller, &memory, cid_ptr, cid_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let cid_str = match std::str::from_utf8(&cid_bytes) {
        Ok(s) => s,
        Err(_) => return GuestErrorCode::InvalidId.as_i32(),
    };

    match host_ctx.storage.delete(&host_ctx.capabilities, cid_str) {
        Ok(deleted) => {
            if deleted {
                1
            } else {
                0
            }
        }
        Err(e) => e.code().as_i32(),
    }
}

fn handle_service_resolve_raw(
    caller: &mut Caller<'_, StoreData>,
    query_ptr: i32,
    query_len: i32,
    out_desc_ptr: i32,
    out_desc_max_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let req_bytes = match read_guest_memory(caller, &memory, query_ptr, query_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let req: ServiceResolveRequest = match serde_json::from_slice(&req_bytes) {
        Ok(r) => r,
        Err(_) => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let services = match host_ctx.services.as_ref() {
        Some(s) => s,
        None => return GuestErrorCode::ServiceUnavailable.as_i32(),
    };

    match services.resolve(&host_ctx.capabilities, &req) {
        Ok(desc) => {
            let json_bytes = match serde_json::to_vec(&desc) {
                Ok(b) => b,
                Err(_) => return GuestErrorCode::InternalError.as_i32(),
            };
            match write_guest_memory(caller, &memory, out_desc_ptr, out_desc_max_len, &json_bytes) {
                Ok(written) => written,
                Err(e) => e.as_i32(),
            }
        }
        Err(e) => e.code().as_i32(),
    }
}

fn handle_service_describe_raw(
    caller: &mut Caller<'_, StoreData>,
    svc_id_ptr: i32,
    svc_id_len: i32,
    out_desc_ptr: i32,
    out_desc_max_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let bytes = match read_guest_memory(caller, &memory, svc_id_ptr, svc_id_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let svc_id = match std::str::from_utf8(&bytes) {
        Ok(s) => s,
        Err(_) => return GuestErrorCode::InvalidId.as_i32(),
    };
    let services = match host_ctx.services.as_ref() {
        Some(s) => s,
        None => return GuestErrorCode::ServiceUnavailable.as_i32(),
    };

    match services.describe(&host_ctx.capabilities, svc_id) {
        Ok(desc) => {
            let json_bytes = match serde_json::to_vec(&desc) {
                Ok(b) => b,
                Err(_) => return GuestErrorCode::InternalError.as_i32(),
            };
            match write_guest_memory(caller, &memory, out_desc_ptr, out_desc_max_len, &json_bytes) {
                Ok(written) => written,
                Err(e) => e.as_i32(),
            }
        }
        Err(e) => e.code().as_i32(),
    }
}

fn handle_event_publish_raw(
    caller: &mut Caller<'_, StoreData>,
    topic_ptr: i32,
    topic_len: i32,
    payload_ptr: i32,
    payload_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let topic_bytes = match read_guest_memory(caller, &memory, topic_ptr, topic_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let topic_str = match std::str::from_utf8(&topic_bytes) {
        Ok(s) => s,
        Err(_) => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let payload = match read_guest_memory(caller, &memory, payload_ptr, payload_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };

    match host_ctx
        .events
        .publish(&host_ctx.capabilities, topic_str, &payload)
    {
        Ok(_) => 0, // Success
        Err(e) => e.code().as_i32(),
    }
}

fn handle_network_send_raw(
    caller: &mut Caller<'_, StoreData>,
    dest_ptr: i32,
    dest_len: i32,
    payload_ptr: i32,
    payload_len: i32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return GuestErrorCode::InvalidRequest.as_i32(),
    };
    let host_ctx = match caller.data().host_context.clone() {
        Some(ctx) => ctx,
        None => return GuestErrorCode::InternalError.as_i32(),
    };
    let dest_bytes = match read_guest_memory(caller, &memory, dest_ptr, dest_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };
    let dest_str = match std::str::from_utf8(&dest_bytes) {
        Ok(s) => s,
        Err(_) => return GuestErrorCode::InvalidId.as_i32(),
    };
    let payload = match read_guest_memory(caller, &memory, payload_ptr, payload_len) {
        Ok(d) => d,
        Err(e) => return e.as_i32(),
    };

    let network = match host_ctx.network.as_ref() {
        Some(n) => n,
        None => return GuestErrorCode::NetworkUnavailable.as_i32(),
    };

    let req = NetworkSendRequest {
        destination: dest_str.to_string(),
        message_type: "DATA".to_string(),
        payload,
        metadata: None,
        ttl: None,
        expires_in_ms: None,
    };

    match network.send(&host_ctx.capabilities, &req) {
        Ok(_) => 0, // Success
        Err(e) => e.code().as_i32(),
    }
}
