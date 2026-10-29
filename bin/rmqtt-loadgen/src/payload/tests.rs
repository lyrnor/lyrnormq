use super::*;
#[test]fn roundtrip_and_corruption(){let h=Header{run_id:5,pair:3,measured:true,sequence:7,scheduled_ns:10,send_started_ns:12};let mut b=encode(128,h).unwrap();assert_eq!(b.len(),128);assert_eq!(decode(&b).unwrap(),h);b[90]^=1;assert!(decode(&b).is_err());assert!(decode(&b[..20]).is_err());assert!(encode(47,h).is_err());}
