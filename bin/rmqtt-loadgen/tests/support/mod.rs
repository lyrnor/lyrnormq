//! Independent raw MQTT test peer: intentionally does not import mqtt-protocol.
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

#[derive(Clone, Copy, Debug, Default)]
pub enum Fault {
    #[default]
    None,
    WrongAck,
    RejectAck,
    DropSource,
    StallSource,
    CorruptPayload,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Scenario {
    pub source_comp_delay: Duration,
    pub delivery_delay: Duration,
    pub subscriber_rel_delay: Duration,
    pub duplicate_delivery: bool,
    pub fault: Fault,
}
#[derive(Default)]
pub struct Observed {
    pub publications: AtomicUsize,
    pub payload_lengths: Mutex<Vec<usize>>,
    pub versions: Mutex<Vec<u8>>,
    pub subscribed: AtomicBool,
    pub client_ids: Mutex<Vec<String>>,
}
pub struct Peer {
    pub address: std::net::SocketAddr,
    pub observed: Arc<Observed>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Vec<String>>,
}
#[derive(Debug)]
struct Publication {
    topic: Vec<u8>,
    payload: Vec<u8>,
    qos: u8,
    done: Option<oneshot::Sender<()>>,
}

impl Peer {
    pub async fn start(scenario: Scenario) -> Self {
        Self::start_pairs(scenario, 1).await
    }
    pub async fn start_pairs(scenario: Scenario, pairs: usize) -> Self {
        Self::start_with_routes(scenario, pairs, Arc::new(Mutex::new(BTreeMap::new()))).await
    }
    pub async fn start_nodes(scenario: Scenario, nodes: usize, pairs_per_node: usize) -> Vec<Self> {
        let routes = Arc::new(Mutex::new(BTreeMap::new()));
        let mut peers = Vec::new();
        for _ in 0..nodes {
            peers.push(Self::start_with_routes(scenario, pairs_per_node, routes.clone()).await);
        }
        peers
    }
    async fn start_with_routes(scenario: Scenario, pairs: usize, routes: Routes) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let observed = Arc::new(Observed::default());
        let obs = observed.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            let mut errors = Vec::new();
            let mut accepted = 0;
            loop {
                tokio::select! {
                 _=&mut stopped=>break,
                 result=listener.accept(),if accepted<pairs*2=>{match result{Ok((stream,_))=>{accepted+=1;tasks.spawn(connection(stream,scenario,obs.clone(),routes.clone()));},Err(e)=>{errors.push(e.to_string());break;}}},
                 result=tasks.join_next(),if !tasks.is_empty()=>{match result {Some(Ok(Err(e)))=>errors.push(e.to_string()),Some(Err(e))=>errors.push(e.to_string()),_=>{}}}
                }
            }
            tasks.abort_all();
            while let Some(r) = tasks.join_next().await {
                if let Ok(Err(e)) = r {
                    errors.push(e.to_string());
                }
            }
            errors
        });
        Self {
            address,
            observed,
            stop,
            task,
        }
    }
    pub async fn finish(self) -> Vec<String> {
        let _ = self.stop.send(());
        self.task.await.unwrap()
    }
}
fn bad(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
async fn read(stream: &mut TcpStream) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut h = [0u8; 1];
    match timeout(Duration::from_secs(2), stream.read_exact(&mut h)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e))
            if matches!(
                e.kind(),
                io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(bad("oracle read timeout")),
    }
    let mut n = 0usize;
    let mut factor = 1;
    let mut complete = false;
    for _ in 0..4 {
        let b = stream.read_u8().await?;
        n += usize::from(b & 127) * factor;
        if b & 128 == 0 {
            complete = true;
            break;
        }
        factor *= 128;
    }
    if !complete || n > 65536 {
        return Err(bad("oracle received invalid frame length"));
    }
    let mut body = vec![0; n];
    stream.read_exact(&mut body).await?;
    Ok(Some((h[0], body)))
}
async fn write(stream: &mut TcpStream, h: u8, body: &[u8]) -> io::Result<()> {
    let mut bytes = vec![h];
    let mut n = body.len();
    loop {
        let v = (n % 128) as u8;
        n /= 128;
        bytes.push(v | if n > 0 { 128 } else { 0 });
        if n == 0 {
            break;
        }
    }
    bytes.extend_from_slice(body);
    stream.write_all(&bytes).await
}
fn take_id(body: &[u8]) -> io::Result<u16> {
    if body.len() < 2 {
        return Err(bad("missing identifier"));
    }
    Ok(u16::from_be_bytes([body[0], body[1]]))
}
fn skip_props(body: &[u8], position: &mut usize) -> io::Result<()> {
    let mut n = 0usize;
    let mut factor = 1;
    for _ in 0..4 {
        let v = *body
            .get(*position)
            .ok_or_else(|| bad("missing properties"))?;
        *position += 1;
        n += usize::from(v & 127) * factor;
        if v & 128 == 0 {
            *position = position
                .checked_add(n)
                .ok_or_else(|| bad("properties overflow"))?;
            if *position > body.len() {
                return Err(bad("truncated properties"));
            }
            return Ok(());
        }
        factor *= 128;
    }
    Err(bad("malformed properties"))
}
fn publication(h: u8, body: &[u8], version: u8) -> io::Result<(Publication, Option<u16>)> {
    let topic_len = usize::from(take_id(body)?);
    let mut pos = 2 + topic_len;
    let topic = body
        .get(2..pos)
        .ok_or_else(|| bad("truncated topic"))?
        .to_vec();
    let qos = (h >> 1) & 3;
    let id = if qos > 0 {
        let id = take_id(body.get(pos..).ok_or_else(|| bad("truncated ID"))?)?;
        pos += 2;
        Some(id)
    } else {
        None
    };
    if version == 5 {
        skip_props(body, &mut pos)?;
    }
    Ok((
        Publication {
            topic,
            payload: body[pos..].to_vec(),
            qos,
            done: None,
        },
        id,
    ))
}
async fn ack(stream: &mut TcpStream, h: u8, id: u16, version: u8, reason: u8) -> io::Result<()> {
    let mut b = id.to_be_bytes().to_vec();
    if version == 5 && reason != 0 {
        b.extend_from_slice(&[reason, 0]);
    }
    write(stream, h, &b).await
}
async fn expect_id(stream: &mut TcpStream, h: u8, id: u16) -> io::Result<()> {
    match read(stream).await? {
        Some((got, b)) if got == h && take_id(&b)? == id => Ok(()),
        Some(_) => Err(bad("unexpected subscriber handshake")),
        None => Err(bad("subscriber closed during handshake")),
    }
}
async fn deliver(
    stream: &mut TcpStream,
    p: Publication,
    version: u8,
    scenario: Scenario,
) -> io::Result<()> {
    tokio::time::sleep(scenario.delivery_delay).await;
    let id: u16 = 30001;
    let mut body = (p.topic.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(&p.topic);
    if p.qos > 0 {
        body.extend_from_slice(&id.to_be_bytes());
    }
    if version == 5 {
        body.push(0);
    }
    let done = p.done;
    let mut payload = p.payload;
    if matches!(scenario.fault, Fault::CorruptPayload)
        && let Some(last) = payload.last_mut()
    {
        *last ^= 1;
    }
    body.extend_from_slice(&payload);
    write(stream, 0x30 | (p.qos << 1), &body).await?;
    match p.qos {
        0 => {}
        1 => {
            expect_id(stream, 0x40, id).await?;
            if scenario.duplicate_delivery {
                write(stream, 0x3a, &body).await?;
                expect_id(stream, 0x40, id).await?;
            }
        }
        2 => {
            expect_id(stream, 0x50, id).await?;
            tokio::time::sleep(scenario.subscriber_rel_delay).await;
            ack(stream, 0x62, id, version, 0).await?;
            expect_id(stream, 0x70, id).await?;
        }
        _ => return Err(bad("invalid QoS")),
    }
    if let Some(done) = done {
        let _ = done.send(());
    }
    Ok(())
}
type Routes = Arc<Mutex<BTreeMap<Vec<u8>, mpsc::Sender<Publication>>>>;
async fn route(routes: &Routes, publication: Publication) -> io::Result<()> {
    let target = routes
        .lock()
        .unwrap()
        .get(&publication.topic)
        .cloned()
        .ok_or_else(|| bad("PUBLISH topic has no completed subscription"))?;
    target
        .send(publication)
        .await
        .map_err(|_| bad("subscriber gone"))
}
async fn connection(
    mut stream: TcpStream,
    scenario: Scenario,
    obs: Arc<Observed>,
    routes: Routes,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let Some((h, b)) = read(&mut stream).await? else {
        return Ok(());
    };
    if h != 0x10 || b.len() < 10 || &b[..6] != b"\0\x04MQTT" {
        return Err(bad("invalid CONNECT"));
    }
    let version = b[6];
    if !matches!(version, 4 | 5) {
        return Err(bad("unsupported test version"));
    }
    obs.versions.lock().unwrap().push(version);
    let mut pos = 10;
    if version == 5 {
        skip_props(&b, &mut pos)?;
    }
    let len = usize::from(take_id(
        b.get(pos..).ok_or_else(|| bad("missing client ID"))?,
    )?);
    pos += 2;
    let client_id = std::str::from_utf8(
        b.get(pos..pos + len)
            .ok_or_else(|| bad("truncated client ID"))?,
    )
    .map_err(|_| bad("non-UTF8 client ID"))?;
    obs.client_ids.lock().unwrap().push(client_id.to_owned());
    write(
        &mut stream,
        0x20,
        if version == 5 { &[0, 0, 0] } else { &[0, 0] },
    )
    .await?;
    let mut pending = BTreeMap::new();
    while let Some((h, b)) = read(&mut stream).await? {
        match h >> 4 {
            8 => {
                if h != 0x82 {
                    return Err(bad("SUBSCRIBE flags"));
                }
                let id = take_id(&b)?;
                let mut pos = 2;
                if version == 5 {
                    skip_props(&b, &mut pos)?;
                }
                let len = usize::from(take_id(b.get(pos..).ok_or_else(|| bad("filter field"))?)?);
                pos += 2;
                let end = pos + len;
                let filter = b.get(pos..end).ok_or_else(|| bad("filter data"))?.to_vec();
                let (sender, mut messages) = mpsc::channel(16);
                if routes.lock().unwrap().insert(filter, sender).is_some() {
                    return Err(bad("duplicate exact subscription"));
                }
                let qos = *b.get(end).ok_or_else(|| bad("subscription options"))? & 3;
                let mut answer = id.to_be_bytes().to_vec();
                if version == 5 {
                    answer.push(0);
                }
                answer.push(qos);
                write(&mut stream, 0x90, &answer).await?;
                obs.subscribed.store(true, Ordering::SeqCst);
                loop {
                    tokio::select! {p=messages.recv()=>{let Some(p)=p else{return Ok(());};if let Err(e)=deliver(&mut stream,p,version,scenario).await{if matches!(scenario.fault,Fault::CorruptPayload){return Ok(());}return Err(e);}},p=read(&mut stream)=>{match p?{Some((0xe0,_))|None=>return Ok(()),Some((0xc0,_))=>write(&mut stream,0xd0,&[]).await?,Some(_)=>return Err(bad("unexpected idle subscriber packet"))}}}
                }
            }
            3 => {
                if !obs.subscribed.load(Ordering::SeqCst) {
                    return Err(bad("PUBLISH before SUBACK"));
                }
                let (mut p, id) = publication(h, &b, version)?;
                obs.publications.fetch_add(1, Ordering::SeqCst);
                obs.payload_lengths.lock().unwrap().push(p.payload.len());
                match scenario.fault {
                    Fault::DropSource => return Ok(()),
                    Fault::StallSource => continue,
                    _ => {}
                }
                let wrong = matches!(scenario.fault, Fault::WrongAck);
                let reason = if matches!(scenario.fault, Fault::RejectAck) {
                    0x97
                } else {
                    0
                };
                match p.qos {
                    0 => route(&routes, p).await?,
                    1 => {
                        if reason == 0 && !wrong {
                            let completed = if scenario.duplicate_delivery {
                                let (tx, rx) = oneshot::channel();
                                p.done = Some(tx);
                                Some(rx)
                            } else {
                                None
                            };
                            route(&routes, p).await?;
                            // Keep source completion pending until the duplicate has been
                            // acknowledged, so normal end-of-run closure cannot race it.
                            if let Some(completed) = completed {
                                completed
                                    .await
                                    .map_err(|_| bad("duplicate handshake incomplete"))?;
                            }
                        }
                        let id = id.ok_or_else(|| bad("missing ID"))?;
                        ack(
                            &mut stream,
                            0x40,
                            if wrong { id.wrapping_add(1) } else { id },
                            version,
                            reason,
                        )
                        .await?;
                    }
                    2 => {
                        let id = id.ok_or_else(|| bad("missing ID"))?;
                        pending.insert(id, p);
                        ack(&mut stream, 0x50, id, version, 0).await?;
                    }
                    _ => return Err(bad("invalid source QoS")),
                }
            }
            6 => {
                if h != 0x62 {
                    return Err(bad("PUBREL fixed flags"));
                }
                let id = take_id(&b)?;
                if let Some(p) = pending.remove(&id) {
                    route(&routes, p).await?;
                }
                tokio::time::sleep(scenario.source_comp_delay).await;
                ack(&mut stream, 0x70, id, version, 0).await?;
            }
            12 => write(&mut stream, 0xd0, &[]).await?,
            14 => return Ok(()),
            _ => return Err(bad("unexpected source frame")),
        }
    }
    Ok(())
}
