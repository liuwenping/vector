/// MSG_DATA_REPORT
#[allow(dead_code)]
pub const GSE_TYPE_COMMON: u32   = 3072 + 1;
#[allow(dead_code)]
pub const GSE_TYPE_GET_CONF: u32 = 0x0A;      // REPORT_SYNC_CONFIG
#[allow(dead_code)]
pub const GSE_TYPE_DYNAMIC: u32  = 0x09;      // REPORT_DYNAMICAL_PROTOCOL_TYPE
#[allow(dead_code)]
pub const GSE_TYPE_OP: u32       = 3072 + 12; // MSG_DATA_REPORT_OPS
#[allow(dead_code)]
pub const GSE_TYPE_TLOGC: u32    = 0x02;      // REPORT_EXT

#[allow(dead_code)]
pub const GSE_TYPE_DYNAMIC_DEFAULT_META_MAX_LEN: u32 = 408; // keep same with gse (8 + 128) * 3 = 408b
#[allow(dead_code)]
pub const GSE_TYPE_DYNAMIC_EXT_HEAD_LEN: u32         = 24;  // sizeof (index ... metaCount) = 24B
#[allow(dead_code)]
pub const GSE_TYPE_DYNAMIC_META_LEN:u32             = 8;   // len(keyLen) + len(valueLen) = 8B


#[derive(Debug)]
pub struct GseCommonMsgHead {
    pub msg_type: u32,
    pub data_id: u32,
    pub utc_time: u32,
    pub body_len: u32,
    pub resv: [u32; 2],
}



impl GseCommonMsgHead {
    #[allow(dead_code)]
    pub fn new() -> GseCommonMsgHead {
        GseCommonMsgHead {
            msg_type: GSE_TYPE_GET_CONF,
            data_id: 0,
            utc_time: chrono::Utc::now().timestamp() as u32,
            body_len: 0,
            resv: [0; 2],
        }
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        let mut head_msg_type = self.msg_type.to_be_bytes().to_vec();
        let mut head_data_id = self.data_id.to_be_bytes().to_vec();
        let mut head_utc_time = self.utc_time.to_be_bytes().to_vec();
        let mut head_body_len = self.body_len.to_be_bytes().to_vec();
        let mut head_resv0 = self.resv[0].to_be_bytes().to_vec();
        let mut head_resv1 = self.resv[1].to_be_bytes().to_vec();

        // append head
        ret.append(&mut head_msg_type);
        ret.append(&mut head_data_id);
        ret.append(&mut head_utc_time);
        ret.append(&mut head_body_len);
        ret.append(&mut head_resv0);
        ret.append(&mut head_resv1);
        ret
    }
}


#[derive(Debug)]
pub struct GseCommonMsg {
    pub head: GseCommonMsgHead,
    pub data: Vec<u8>,
}


impl GseCommonMsg {
    #[allow(dead_code)]
    pub fn new() -> GseCommonMsg {
        GseCommonMsg {
            head: GseCommonMsgHead::new(),
            data: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();

        // append head
        ret.append(&mut self.head.to_bytes());

        // append body
        ret.append(&mut self.data.clone());

        ret
    }
}


#[derive(Debug)]
pub struct GseDynamicMsgHead {
    pub common_head: GseCommonMsgHead,

    pub index: u64,
    pub flags: u32,
    pub meta_len: u32,
    pub meta_max_len: u32,
    pub meta_count: u32,
}

impl GseDynamicMsgHead {
    pub fn new() -> GseDynamicMsgHead {
        GseDynamicMsgHead {
            common_head: GseCommonMsgHead::new(),
            index: 0,
            flags: 0,
            meta_len: 0,
            meta_count: 0,
            meta_max_len: GSE_TYPE_DYNAMIC_DEFAULT_META_MAX_LEN,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        ret.append(&mut self.common_head.to_bytes());

        // append self head
        let mut head_index = self.index.to_be_bytes().to_vec();
        let mut head_flags = self.flags.to_be_bytes().to_vec();
        let mut head_meta_len = self.meta_len.to_be_bytes().to_vec();
        let mut head_meta_max_len = self.meta_max_len.to_be_bytes().to_vec();
        let mut head_meta_count = self.meta_count.to_be_bytes().to_vec();

        ret.append(&mut head_index);
        ret.append(&mut head_flags);
        ret.append(&mut head_meta_len);
        ret.append(&mut head_meta_max_len);
        ret.append(&mut head_meta_count);

        ret
    }
}

#[derive(Debug)]
pub struct GseDynamicMetaInfo {
    pub key_len: u32,
    pub value_len: u32,
    pub meta_key: String,
    pub meta_value: String,
}


impl GseDynamicMetaInfo {

    #[allow(dead_code)]
    pub fn new() -> GseDynamicMetaInfo {
        GseDynamicMetaInfo {
            key_len: 0,
            value_len: 0,
            meta_key: String::new(),
            meta_value: String::new(),
        }
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();

        let mut info_key_len = self.key_len.to_be_bytes().to_vec();
        let mut info_value_len = self.value_len.to_be_bytes().to_vec();

        ret.append(&mut info_key_len);
        ret.append(&mut info_value_len);
        ret.append(&mut self.meta_key.as_bytes().to_vec());
        ret.append(&mut self.meta_value.as_bytes().to_vec());

        ret
    }
}

#[derive(Debug)]
pub struct GseDynamicMsg {
    pub head: GseDynamicMsgHead,
    pub metas: Vec<GseDynamicMetaInfo>,
    pub data: Vec<u8>,
}

impl GseDynamicMsg {
    #[allow(dead_code)]
    pub fn new() -> GseDynamicMsg {
        GseDynamicMsg{
            head: GseDynamicMsgHead::new(),
            metas: Vec::new(),
            data: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn new_with_data(data: Vec<u8>, data_id: u32) -> GseDynamicMsg {
        let mut msg = GseDynamicMsg::new();
        msg.head.common_head.msg_type = GSE_TYPE_DYNAMIC;
        msg.head.common_head.data_id = data_id;
	    msg.head.common_head.utc_time = chrono::Utc::now().timestamp() as u32;
        msg.head.common_head.resv[0] = 0;
        msg.head.common_head.resv[1] = 0;
        msg.head.meta_len = 0;
        msg.head.meta_count = 0;
        msg.head.meta_max_len = GSE_TYPE_DYNAMIC_DEFAULT_META_MAX_LEN;
        msg.head.meta_count = 0;
        msg.head.common_head.body_len = data.len() as u32 + GSE_TYPE_DYNAMIC_EXT_HEAD_LEN + msg.head.meta_max_len;
        msg.data = data.clone();

        msg
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        ret.append(&mut self.head.to_bytes());

        for meta in &self.metas {
            ret.append(&mut meta.to_bytes())
        }
        let left_space_meta_length = self.head.meta_max_len - self.head.meta_len;
        let mut left_space_meta = vec![0; left_space_meta_length as usize];
        ret.append(&mut left_space_meta);

        ret.append(&mut self.data.clone());

        ret
    }
}

#[derive(Debug)]
pub struct GseResponseMsg {
    pub msg_type: u32,
    pub body_len: u32,
}

impl GseResponseMsg {
    #[allow(dead_code)]
    pub fn parse_header(bytes: [u8; 8]) -> Self {
        let first = &bytes[0..4];
        let second = &bytes[4..8];
        let msg_type = u32::from_be_bytes(first.try_into().unwrap());
        let body_len = u32::from_be_bytes(second.try_into().unwrap());
        GseResponseMsg {
            msg_type,
            body_len,
        }
    }
}
