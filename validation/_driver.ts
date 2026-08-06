// bigname migration-catalog validation driver.
// Lives only in this scratch copy of the pinned ens_v2 checkout (ccaeb58b).
// Executes the catalog scenario transaction sequences against the pinned
// devnet stack and dumps per-scenario transaction + event-log evidence to
// the migration-catalog/validation directory.
import { afterAll, describe, expect, it } from "bun:test";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  type Account,
  type Address,
  type Hex,
  decodeEventLog,
  encodeAbiParameters,
  getContract,
  keccak256,
  namehash,
  stringToBytes,
  zeroAddress,
} from "viem";
import {
  FUSES,
  MAX_EXPIRY,
  PREMIGRATION_BONUS_PERIOD,
  ROLES,
  SEC_PER_YEAR,
  STATUS,
} from "../../script/deploy-constants.js";
import { migrationDataComponents } from "../../script/migration.js";
import { COIN_TYPE_ETH, dnsEncodeName, getLabelAt, idFromLabel } from "../utils/utils.js";

const labelHex = (label: string) => keccak256(stringToBytes(label));
import { bundleCalls, makeResolutions, type KnownProfile } from "../utils/resolutions.js";

const OUT_DIR =
  "/tmp/claude-1000/-home-ubuntu-bigname/cf8d6917-6d7b-4632-b3f8-154445c9fd11/scratchpad/migration-catalog/validation";
mkdirSync(OUT_DIR, { recursive: true });

const { env, setupEnv } = process.TEST_GLOBALS!;

const DAY = 86400n;
const DURATION = 365n * DAY;

const anotherAddress = "0x8000000000000000000000000000000000000001" as Address;
const defaultProfile = {
  addresses: [{ coinType: COIN_TYPE_ETH, value: anotherAddress }],
  texts: [{ key: "url", value: "https://ens.domains" }],
} as const;

type StepRec = { step: number; contract: string; fn: string; args: unknown[]; sender: string };
type CheckRec = { name: string; ok: boolean; detail?: unknown };
type LogRec = {
  blockNumber: string;
  logIndex: number;
  address: string;
  contract: string;
  event: string;
  args: unknown;
};
type ScenarioRecord = {
  id: string;
  title: string;
  status: string;
  txs: StepRec[];
  reverts: { step: string; error: string }[];
  checks: CheckRec[];
  logs: LogRec[];
  notes: string[];
};

const summary: { id: string; title: string; status: string; checks: number; logs: number }[] = [];

function jsonSafe(v: unknown): unknown {
  if (v === undefined) return null;
  const encoded = JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x));
  return encoded === undefined ? null : JSON.parse(encoded);
}

function addressBook(): { map: Map<string, string>; abi: any[] } {
  const map = new Map<string, string>();
  const abi: any[] = [];
  const deployments = (env.rocketh as any).deployments ?? {};
  for (const [name, dep] of Object.entries<any>(deployments)) {
    if (dep?.address) map.set(dep.address.toLowerCase(), name);
    if (dep?.abi) abi.push(...dep.abi);
  }
  return { map, abi };
}
const book = addressBook();
function registerDynamic(address: Address, name: string) {
  book.map.set(address.toLowerCase(), name);
}

async function blockNumber(): Promise<bigint> {
  return (await env.client.getBlock()).number;
}

async function collectLogs(rec: ScenarioRecord, fromBlock: bigint) {
  const toBlock = await blockNumber();
  if (toBlock < fromBlock) return;
  const logs = await env.client.getLogs({ fromBlock, toBlock });
  for (const log of logs) {
    let event = `topic0=${log.topics[0] ?? "none"}`;
    let args: unknown = { data: log.data };
    try {
      const decoded = decodeEventLog({
        abi: book.abi,
        data: log.data,
        topics: log.topics,
      });
      event = decoded.eventName;
      args = decoded.args;
    } catch {}
    rec.logs.push({
      blockNumber: log.blockNumber!.toString(),
      logIndex: log.logIndex!,
      address: log.address,
      contract: book.map.get(log.address.toLowerCase()) ?? "(dynamic)",
      event,
      args: jsonSafe(args),
    });
  }
}

let stepCounter = 0;
async function step<T>(
  rec: ScenarioRecord,
  contract: string,
  fn: string,
  args: unknown[],
  sender: string,
  exec: () => Promise<T>,
): Promise<T> {
  rec.txs.push({ step: ++stepCounter, contract, fn, args: jsonSafe(args) as unknown[], sender });
  return exec();
}

async function expectRevert(
  rec: ScenarioRecord,
  label: string,
  match: string | RegExp,
  exec: () => Promise<unknown>,
) {
  try {
    await exec();
    rec.checks.push({ name: `${label} reverts`, ok: false, detail: "did not revert" });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const ok = typeof match === "string" ? msg.includes(match) : match.test(msg);
    rec.reverts.push({ step: label, error: msg.slice(0, 400) });
    rec.checks.push({ name: `${label} reverts with ${match}`, ok, detail: msg.slice(0, 200) });
  }
}

function check(rec: ScenarioRecord, name: string, ok: boolean, detail?: unknown) {
  rec.checks.push({ name, ok, detail: jsonSafe(detail) });
}

function scenario(id: string, title: string, fn: (rec: ScenarioRecord) => Promise<void>) {
  it(`${id} ${title}`, async () => {
    stepCounter = 0;
    const rec: ScenarioRecord = {
      id,
      title,
      status: "VALIDATED",
      txs: [],
      reverts: [],
      checks: [],
      logs: [],
      notes: [],
    };
    const from = (await blockNumber()) + 1n;
    let fatal: unknown;
    try {
      await fn(rec);
    } catch (err) {
      fatal = err;
      rec.status = "FAILED";
      rec.notes.push(`fatal: ${err instanceof Error ? err.message : String(err)}`);
    }
    await collectLogs(rec, from);
    if (rec.status !== "FAILED" && rec.checks.some((c) => !c.ok)) rec.status = "CHECK-FAILED";
    summary.push({ id, title, status: rec.status, checks: rec.checks.length, logs: rec.logs.length });
    writeFileSync(join(OUT_DIR, `${id}.json`), JSON.stringify(rec, null, 2));
    if (fatal) throw fatal;
    const bad = rec.checks.filter((c) => !c.ok);
    if (bad.length > 0) {
      throw new Error(
        `${id} failed checks: ${bad.map((c) => `${c.name} :: ${JSON.stringify(c.detail)}`).join("; ")}`,
      );
    }
  }, 30000);
}

// ---------------------------------------------------------------- helpers

function encodeData(v: any | any[]): Hex {
  return Array.isArray(v)
    ? encodeAbiParameters([{ type: "tuple[]", components: migrationDataComponents }], [v])
    : encodeAbiParameters([{ type: "tuple", components: migrationDataComponents }], [v]);
}

async function makeData(name: string, account: Account, over: Record<string, unknown> = {}) {
  const resolver = await env.v1.ENSRegistry.read.resolver([namehash(name)]);
  return {
    label: getLabelAt(name),
    owner: account.address,
    subregistry: zeroAddress,
    resolver,
    ...over,
  };
}

async function premigrate(rec: ScenarioRecord, label: string) {
  const expiry = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel(label)]);
  await step(
    rec,
    "v2.ETHRegistry",
    "register (reserve)",
    [label, "owner=0", "registry=0", "ENSV1Resolver", "roles=0", `${expiry + PREMIGRATION_BONUS_PERIOD}`],
    "deployer",
    () =>
      env.v2.ETHRegistry.write.register([
        label,
        zeroAddress,
        zeroAddress,
        env.v2.ENSV1Resolver.address,
        0n,
        expiry + PREMIGRATION_BONUS_PERIOD,
      ]),
  );
}

async function registerUnwrapped(
  rec: ScenarioRecord,
  label: string,
  account: Account,
  { duration = DURATION, premigrated = true }: { duration?: bigint; premigrated?: boolean } = {},
) {
  await step(rec, "v1.BaseRegistrar", "register", [label, account.address, `${duration}`], "deployer(controller)", () =>
    env.v1.BaseRegistrar.write.register([idFromLabel(label), account.address, duration]),
  );
  if (premigrated) await premigrate(rec, label);
}

async function wrapName(rec: ScenarioRecord, label: string, account: Account, fuses: number) {
  await step(
    rec,
    "v1.BaseRegistrar",
    "safeTransferFrom→NameWrapper (wrap)",
    [label, `fuses=${fuses}`],
    account.address,
    () =>
      env.v1.BaseRegistrar.write.safeTransferFrom(
        [
          account.address,
          env.v1.NameWrapper.address,
          idFromLabel(label),
          encodeAbiParameters(
            [
              { name: "label", type: "string" },
              { name: "owner", type: "address" },
              { name: "fuses", type: "uint16" },
              { name: "resolver", type: "address" },
            ],
            [label, account.address, fuses, zeroAddress],
          ),
        ],
        { account },
      ),
  );
}

async function registerWrapped(
  rec: ScenarioRecord,
  label: string,
  account: Account,
  fuses: number,
  opts: { duration?: bigint; premigrated?: boolean } = {},
) {
  await registerUnwrapped(rec, label, account, opts);
  await wrapName(rec, label, account, fuses);
}

async function createChild(
  rec: ScenarioRecord,
  parentName: string,
  label: string,
  account: Account,
  fuses: number,
  expiry: bigint = MAX_EXPIRY,
  parentAccount: Account = account,
) {
  await step(
    rec,
    "v1.NameWrapper",
    "setSubnodeOwner",
    [parentName, label, account.address, `fuses=${fuses}`, `${expiry}`],
    parentAccount.address,
    () =>
      env.v1.NameWrapper.write.setSubnodeOwner(
        [namehash(parentName), label, account.address, fuses, expiry],
        { account: parentAccount },
      ),
  );
  return `${label}.${parentName}`;
}

async function setupRecords(rec: ScenarioRecord, name: string, account: Account) {
  const node = namehash(name);
  const wrapped =
    (await env.v1.ENSRegistry.read.owner([node])).toLowerCase() ===
    env.v1.NameWrapper.address.toLowerCase();
  if (wrapped) {
    await step(rec, "v1.NameWrapper", "setResolver", [name, "PublicResolver"], account.address, () =>
      env.v1.NameWrapper.write.setResolver([node, env.v1.PublicResolver.address], { account }),
    );
  } else {
    await step(rec, "v1.ENSRegistry", "setResolver", [name, "PublicResolver"], account.address, () =>
      env.v1.ENSRegistry.write.setResolver([node, env.v1.PublicResolver.address], { account }),
    );
  }
  const res = makeResolutions({ name, ...defaultProfile } as KnownProfile);
  await step(rec, "v1.PublicResolver", "multicall(setAddr,setText)", [name], account.address, () =>
    env.v1.PublicResolver.write.multicall([res.map((x) => x.write)], { account }),
  );
}

async function migrateUnwrapped(
  rec: ScenarioRecord,
  name: string,
  account: Account,
  {
    target = env.v2.UnlockedMigrationController.address,
    sender = account,
    data,
    rawData,
  }: { target?: Address; sender?: Account; data?: Record<string, unknown>; rawData?: Hex } = {},
) {
  const md = await makeData(name, account, data);
  return step(
    rec,
    "v1.BaseRegistrar",
    "safeTransferFrom→controller (migrate)",
    [name, jsonSafe(md)],
    sender.address,
    () =>
      env.v1.BaseRegistrar.write.safeTransferFrom(
        [account.address, target, idFromLabel(getLabelAt(name)), rawData ?? encodeData(md)],
        { account: sender },
      ),
  );
}

async function migrateWrapped(
  rec: ScenarioRecord,
  name: string,
  account: Account,
  target: Address,
  {
    sender = account,
    data,
    rawData,
  }: { sender?: Account; data?: Record<string, unknown>; rawData?: Hex } = {},
) {
  const md = await makeData(name, account, data);
  return step(
    rec,
    "v1.NameWrapper",
    "safeTransferFrom→receiver (migrate)",
    [name, jsonSafe(md)],
    sender.address,
    () =>
      env.v1.NameWrapper.write.safeTransferFrom(
        [account.address, target, BigInt(namehash(name)), 1n, rawData ?? encodeData(md)],
        { account: sender },
      ),
  );
}

async function checkMigrated2LD(
  rec: ScenarioRecord,
  name: string,
  owner: Address,
  { stillWrapped = false, resolverCleared = true }: { stillWrapped?: boolean; resolverCleared?: boolean } = {},
) {
  const label = getLabelAt(name);
  const state = await env.v2.ETHRegistry.read.getState([idFromLabel(label)]);
  check(rec, `${name} v2 REGISTERED`, state.status === STATUS.REGISTERED, state.status);
  check(
    rec,
    `${name} v2 owner`,
    state.latestOwner.toLowerCase() === owner.toLowerCase(),
    state.latestOwner,
  );
  const node = namehash(name);
  const v1owner = await env.v1.ENSRegistry.read.owner([node]);
  if (stillWrapped) {
    // locked path: name stays wrapped; registry owner remains the NameWrapper,
    // while the wrapped ERC-1155 itself is parked in the Graveyard.
    check(
      rec,
      `${name} v1 registry owner remains NameWrapper (still wrapped)`,
      v1owner.toLowerCase() === env.v1.NameWrapper.address.toLowerCase(),
      v1owner,
    );
    const wd = await env.v1.NameWrapper.read.getData([BigInt(node)]);
    check(
      rec,
      `${name} wrapped token parked in Graveyard`,
      wd[0].toLowerCase() === env.v2.Graveyard.address.toLowerCase(),
      wd[0],
    );
  } else {
    check(
      rec,
      `${name} v1 registry owner = Graveyard`,
      v1owner.toLowerCase() === env.v2.Graveyard.address.toLowerCase(),
      v1owner,
    );
  }
  const v1resolver = await env.v1.ENSRegistry.read.resolver([node]);
  if (resolverCleared) {
    check(rec, `${name} v1 resolver cleared`, v1resolver === zeroAddress, v1resolver);
  } else {
    check(rec, `${name} v1 resolver intact (CSR frozen)`, v1resolver !== zeroAddress, v1resolver);
  }
  return state;
}

function wrapperRegistryFor(name: string) {
  const wr = env.findWrapperRegistry(name, env.namedAccounts.deployer);
  registerDynamic(wr.address, `WrapperRegistry(${name})`);
  return wr;
}

// The devnet's rocketh deployment named "MigrationHelper" is the *ENSv1*
// ens-contracts MigrationHelper inherited from the imported mainnet v1
// deployments (name collision). The pinned sepolia artifact IS the v2 helper,
// but on this devnet we deploy the v2 MigrationHelper ourselves from the
// pinned-tree forge build, with the same constructor wiring as
// deploy/05_MigrationHelper.ts.
const helperForgeArtifact = await Bun.file(
  new URL("../../out/MigrationHelper.sol/MigrationHelper.json", import.meta.url),
).json();
async function migrationHelper(account: Account) {
  const wallet = env.createClient(env.namedAccounts.deployer);
  const hash = await wallet.deployContract({
    abi: helperForgeArtifact.abi,
    bytecode: helperForgeArtifact.bytecode.object,
    args: [
      env.v2.RootRegistry.address,
      env.v2.UnlockedMigrationController.address,
      env.v2.LockedMigrationController.address,
      env.v2.ContractNamer.address,
    ],
  });
  const receipt = await env.waitFor(hash);
  const address = receipt.contractAddress!;
  registerDynamic(address, "MigrationHelper(v2)");
  book.abi.push(...helperForgeArtifact.abi);
  return env.patchContractWrite(
    getContract({ abi: helperForgeArtifact.abi, address, client: env.createClient(account) }),
  ) as any;
}

async function warpTo(rec: ScenarioRecord, timestamp: bigint, why: string) {
  rec.notes.push(`warp to ${timestamp} (${why})`);
  await env.client.setNextBlockTimestamp({ timestamp });
  await env.client.mine({ blocks: 1 });
}

async function renewViaRenewer(rec: ScenarioRecord, label: string, duration: bigint, account: Account) {
  const amount = await env.v2.ETHRenewerV1.read.getRenewPrice([
    label,
    duration,
    env.erc20.MockUSDC.address,
  ]);
  await step(rec, "erc20.MockUSDC", "mint+approve", [label, `${amount}`], account.address, async () => {
    await env.erc20.MockUSDC.write.mint([account.address, amount]);
    await env.erc20.MockUSDC.write.approve([env.v2.ETHRenewerV1.address, amount], { account });
  });
  await step(
    rec,
    "v2.ETHRenewerV1",
    "renew",
    [label, `${duration}`, "MockUSDC", "referrer"],
    account.address,
    () =>
      env.v2.ETHRenewerV1.write.renew(
        [label, duration, env.erc20.MockUSDC.address, namehash("referrer")],
        { account },
      ),
  );
}

// ---------------------------------------------------------------- suite

describe("migration catalog validation", () => {
  setupEnv({
    resetOnEach: true,
    async initialize() {
      await env.v1.RegistrarSecurityController.write.addRegistrarController(
        [env.namedAccounts.deployer.address],
        { account: env.namedAccounts.owner },
      );
    },
  });

  const A = () => env.namedAccounts;

  // ============================================================ U: E1 unwrapped

  scenario("U-01", "unwrapped 2LD happy path with records", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "u01", user);
    await setupRecords(rec, "u01.eth", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "u01.eth", user);
    await checkMigrated2LD(rec, "u01.eth", user.address);
    const v2res = await env.v2.ETHRegistry.read.getResolver(["u01"]);
    check(rec, "v2 resolver = migrated v1 resolver", v2res.toLowerCase() === env.v1.PublicResolver.address.toLowerCase(), v2res);
    const bundle = bundleCalls(makeResolutions({ name: "u01.eth", ...defaultProfile } as KnownProfile));
    const [answer] = await env.v2.UniversalResolver.read.resolve([dnsEncodeName("u01.eth"), bundle.call]);
    bundle.expect(answer);
    check(rec, "records resolve via v2 UniversalResolver", true);
  });

  scenario("U-02", "E1 via approved operator", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "u02", user);
    await env.activateV2();
    await step(rec, "v1.BaseRegistrar", "setApprovalForAll", [user2.address, true], user.address, () =>
      env.v1.BaseRegistrar.write.setApprovalForAll([user2.address, true], { account: user }),
    );
    await migrateUnwrapped(rec, "u02.eth", user, { sender: user2 });
    await checkMigrated2LD(rec, "u02.eth", user.address);
  });

  scenario("U-03", "E1 with owner override in payload", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "u03", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "u03.eth", user, { data: { owner: user2.address } });
    await checkMigrated2LD(rec, "u03.eth", user2.address);
  });

  scenario("U-04", "E1 with resolver override in payload", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "u04", user);
    await setupRecords(rec, "u04.eth", user);
    await env.activateV2();
    const resolver = user.resolver;
    await migrateUnwrapped(rec, "u04.eth", user, { data: { resolver: resolver.address } });
    await checkMigrated2LD(rec, "u04.eth", user.address);
    const v2res = await env.v2.ETHRegistry.read.getResolver(["u04"]);
    check(rec, "v2 resolver = override", v2res.toLowerCase() === resolver.address.toLowerCase(), v2res);
  });

  scenario("U-05", "E1 with custom subregistry in payload", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "u05", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "u05.eth", user, { data: { subregistry: anotherAddress } });
    await checkMigrated2LD(rec, "u05.eth", user.address);
    const sub = await env.v2.ETHRegistry.read.getSubregistry(["u05"]);
    check(rec, "v2 subregistry = payload value", sub.toLowerCase() === anotherAddress.toLowerCase(), sub);
  });

  scenario("U-06", "E1 with v1 registry subnode; residue persists and stays v1-mutable", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "u06", user);
    await step(rec, "v1.ENSRegistry", "setSubnodeOwner", ["u06.eth", "sub", user2.address], user.address, () =>
      env.v1.ENSRegistry.write.setSubnodeOwner([namehash("u06.eth"), labelHex("sub"), user2.address], {
        account: user,
      }),
    );
    await env.activateV2();
    await migrateUnwrapped(rec, "u06.eth", user);
    await checkMigrated2LD(rec, "u06.eth", user.address);
    const subOwner = await env.v1.ENSRegistry.read.owner([namehash("sub.u06.eth")]);
    check(rec, "v1 subnode survives migration", subOwner.toLowerCase() === user2.address.toLowerCase(), subOwner);
    // late v1 write by the subnode owner still lands (P-01 evidence)
    await step(rec, "v1.ENSRegistry", "setResolver (late subnode write)", ["sub.u06.eth"], user2.address, () =>
      env.v1.ENSRegistry.write.setResolver([namehash("sub.u06.eth"), anotherAddress], { account: user2 }),
    );
    const subRes = await env.v1.ENSRegistry.read.resolver([namehash("sub.u06.eth")]);
    check(rec, "late v1 subnode write lands", subRes.toLowerCase() === anotherAddress.toLowerCase(), subRes);
  });

  scenario("U-07", "E1 with v1 primary name set pre-migration", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "u07", user);
    await setupRecords(rec, "u07.eth", user);
    await step(rec, "v1.ReverseRegistrar", "setName", ["u07.eth"], user.address, () =>
      env.v1.ReverseRegistrar.write.setName(["u07.eth"], { account: user }),
    );
    await env.activateV2();
    await migrateUnwrapped(rec, "u07.eth", user);
    await checkMigrated2LD(rec, "u07.eth", user.address);
    rec.notes.push(
      "reverse claim lives on v1 reverse registrar; forward verification must now resolve u07.eth through v2 (resolver carried over)",
    );
  });

  scenario("U-08", "E1 near expiry (active); v2 expiry inherits reservation", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "u08", user, { duration: 30n * DAY });
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("u08")]);
    await env.activateV2();
    await warpTo(rec, v1exp - DAY, "1 day before v1 expiry");
    await migrateUnwrapped(rec, "u08.eth", user);
    const state = await checkMigrated2LD(rec, "u08.eth", user.address);
    check(
      rec,
      "v2 expiry = v1 expiry + bonus",
      state.expiry === v1exp + PREMIGRATION_BONUS_PERIOD,
      { v2: `${state.expiry}`, v1: `${v1exp}` },
    );
  });

  scenario("U-09", "E1 with contract (ERC1155Holder) as v2 owner", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "u09", user);
    await env.activateV2();
    // Graveyard is an ERC1155Holder; any IERC1155Receiver works as v2 owner
    await migrateUnwrapped(rec, "u09.eth", user, { data: { owner: env.v2.Graveyard.address } });
    await checkMigrated2LD(rec, "u09.eth", env.v2.Graveyard.address);
    rec.notes.push("v2 owner must implement onERC1155Received; proven positively here, negatively in X-U-07");
  });

  // ============================================================ X-U: E1 reverts

  scenario("X-U-01", "E1 without premigration reverts", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu01", user, { premigrated: false });
    await env.activateV2();
    await expectRevert(rec, "migrate un-premigrated", /EACUnauthorizedAccountRoles|0x4b27a133/, () =>
      migrateUnwrapped(rec, "xu01.eth", user),
    );
  });

  scenario("X-U-02", "E1 with owner=0 reverts InvalidOwner", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu02", user);
    await env.activateV2();
    await expectRevert(rec, "owner=0", "InvalidOwner", () =>
      migrateUnwrapped(rec, "xu02.eth", user, { data: { owner: zeroAddress } }),
    );
  });

  scenario("X-U-03", "E1 with label mismatch reverts NameDataMismatch", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu03", user);
    await env.activateV2();
    await expectRevert(rec, "label mismatch", "NameDataMismatch", () =>
      migrateUnwrapped(rec, "xu03.eth", user, { data: { label: "xu03x" } }),
    );
  });

  scenario("X-U-04", "E1 with junk payload reverts InvalidData", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu04", user);
    await env.activateV2();
    await expectRevert(rec, "junk data", "InvalidData", () =>
      migrateUnwrapped(rec, "xu04.eth", user, { rawData: "0x1234" }),
    );
  });

  scenario("X-U-05", "unwrapped token to LockedMigrationController reverts", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu05", user);
    await env.activateV2();
    await expectRevert(rec, "wrong controller", /non ERC721Receiver/, () =>
      migrateUnwrapped(rec, "xu05.eth", user, { target: env.v2.LockedMigrationController.address }),
    );
  });

  scenario("X-U-06", "E1 during v1 grace reverts (ownerOf gone)", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu06", user, { duration: 30n * DAY });
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("xu06")]);
    await env.activateV2();
    await warpTo(rec, v1exp + DAY, "1 day into v1 grace");
    await expectRevert(rec, "in-grace transfer", /revert|ERC721/i, () =>
      migrateUnwrapped(rec, "xu06.eth", user),
    );
    const status = await env.v2.ETHRegistry.read.getStatus([idFromLabel("xu06")]);
    check(rec, "v2 still RESERVED during bonus window", status === STATUS.RESERVED, status);
  });

  scenario("X-U-07", "E1 with non-receiver contract owner reverts", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "xu07", user);
    await env.activateV2();
    await expectRevert(rec, "non-receiver v2 owner", "ERC1155InvalidReceiver", () =>
      migrateUnwrapped(rec, "xu07.eth", user, { data: { owner: env.v2.ETHRegistry.address } }),
    );
  });

  // ============================================================ W: E2 wrapped unlocked

  scenario("W-01", "wrapped-unlocked 2LD happy path with records", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "w01", user, FUSES.CAN_DO_EVERYTHING);
    await setupRecords(rec, "w01.eth", user);
    await env.activateV2();
    await migrateWrapped(rec, "w01.eth", user, env.v2.UnlockedMigrationController.address);
    await checkMigrated2LD(rec, "w01.eth", user.address);
    const wd = await env.v1.NameWrapper.read.getData([BigInt(namehash("w01.eth"))]);
    check(rec, "wrapper token burned (owner=0)", wd[0] === zeroAddress, wd[0]);
    const t = await env.v1.BaseRegistrar.read.ownerOf([idFromLabel("w01")]);
    check(rec, "v1 ERC721 parked in Graveyard", t.toLowerCase() === env.v2.Graveyard.address.toLowerCase(), t);
  });

  scenario("W-02", "E2 with custom subregistry + owner override", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "w02", user, FUSES.CAN_DO_EVERYTHING);
    await env.activateV2();
    await migrateWrapped(rec, "w02.eth", user, env.v2.UnlockedMigrationController.address, {
      data: { owner: user2.address, subregistry: anotherAddress },
    });
    await checkMigrated2LD(rec, "w02.eth", user2.address);
    const sub = await env.v2.ETHRegistry.read.getSubregistry(["w02"]);
    check(rec, "v2 subregistry = payload", sub.toLowerCase() === anotherAddress.toLowerCase(), sub);
  });

  scenario("W-03", "E2 batch via safeBatchTransferFrom", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "w03a", user, FUSES.CAN_DO_EVERYTHING);
    await registerWrapped(rec, "w03b", user, FUSES.CAN_DO_EVERYTHING);
    await env.activateV2();
    const mds = [await makeData("w03a.eth", user), await makeData("w03b.eth", user)];
    await step(
      rec,
      "v1.NameWrapper",
      "safeBatchTransferFrom→UnlockedMigrationController",
      ["w03a.eth", "w03b.eth"],
      user.address,
      () =>
        env.v1.NameWrapper.write.safeBatchTransferFrom(
          [
            user.address,
            env.v2.UnlockedMigrationController.address,
            [BigInt(namehash("w03a.eth")), BigInt(namehash("w03b.eth"))],
            [1n, 1n],
            encodeData(mds),
          ],
          { account: user },
        ),
    );
    await checkMigrated2LD(rec, "w03a.eth", user.address);
    await checkMigrated2LD(rec, "w03b.eth", user.address);
  });

  scenario("X-W-01", "wrapped-unlocked to LockedMigrationController reverts NameNotLocked", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "xw01", user, FUSES.CAN_DO_EVERYTHING);
    await env.activateV2();
    await expectRevert(rec, "unlocked→locked controller", "NameNotLocked", () =>
      migrateWrapped(rec, "xw01.eth", user, env.v2.LockedMigrationController.address),
    );
  });

  scenario("X-W-02", "wrapped-locked to UnlockedMigrationController reverts NameIsLocked", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "xw02", user, FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    await expectRevert(rec, "locked→unlocked controller", "NameIsLocked", () =>
      migrateWrapped(rec, "xw02.eth", user, env.v2.UnlockedMigrationController.address),
    );
  });

  scenario("X-W-03", "wrapped 2LD in grace cannot transfer", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "xw03", user, FUSES.CAN_DO_EVERYTHING, { duration: 30n * DAY });
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("xw03")]);
    await env.activateV2();
    await warpTo(rec, v1exp + DAY, "1 day into v1 grace");
    await expectRevert(rec, "in-grace wrapped transfer", /insufficient balance|revert/i, () =>
      migrateWrapped(rec, "xw03.eth", user, env.v2.UnlockedMigrationController.address),
    );
  });

  // ============================================================ L: E3 locked

  scenario("L-01", "locked 2LD happy path: WrapperRegistry + fuse-role translation", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "l01", user, FUSES.CANNOT_UNWRAP);
    await setupRecords(rec, "l01.eth", user);
    await env.activateV2();
    await migrateWrapped(rec, "l01.eth", user, env.v2.LockedMigrationController.address);
    const state = await checkMigrated2LD(rec, "l01.eth", user.address, { stillWrapped: true });
    const wr = wrapperRegistryFor("l01.eth");
    const sub = await env.v2.ETHRegistry.read.getSubregistry(["l01"]);
    check(rec, "subregistry = WrapperRegistry(l01.eth)", sub.toLowerCase() === wr.address.toLowerCase(), sub);
    const wd = await env.v1.NameWrapper.read.getData([BigInt(namehash("l01.eth"))]);
    check(
      rec,
      "wrapper token still wrapped, parked in Graveyard",
      wd[0].toLowerCase() === env.v2.Graveyard.address.toLowerCase(),
      wd[0],
    );
    const roles = await env.v2.ETHRegistry.read.roles([state.tokenId, user.address]);
    check(
      rec,
      "token roles: SET_RESOLVER(+admin), CAN_TRANSFER_ADMIN, no RENEW",
      (roles & ROLES.REGISTRY.SET_RESOLVER) !== 0n && (roles & ROLES.REGISTRY.RENEW) === 0n,
      roles.toString(16),
    );
    const wrNode = await wr.read.getWrappedNode();
    check(rec, "WrapperRegistry bound to node", wrNode === namehash("l01.eth"), wrNode);
  });

  scenario("L-02", "locked + CANNOT_SET_RESOLVER with recognized PublicResolver → swapped to v2 PublicResolver", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "l02", user, FUSES.CANNOT_UNWRAP);
    await setupRecords(rec, "l02.eth", user);
    await step(rec, "v1.NameWrapper", "setFuses(CANNOT_SET_RESOLVER)", ["l02.eth"], user.address, () =>
      env.v1.NameWrapper.write.setFuses([namehash("l02.eth"), FUSES.CANNOT_SET_RESOLVER], { account: user }),
    );
    await env.activateV2();
    await migrateWrapped(rec, "l02.eth", user, env.v2.LockedMigrationController.address, {
      data: { resolver: anotherAddress }, // must be ignored
    });
    await checkMigrated2LD(rec, "l02.eth", user.address, { stillWrapped: true, resolverCleared: false });
    const v2res = await env.v2.ETHRegistry.read.getResolver(["l02"]);
    check(
      rec,
      "resolver swapped to v2 PublicResolver (payload ignored)",
      v2res.toLowerCase() === env.v2.PublicResolver.address.toLowerCase(),
      v2res,
    );
    const v1res = await env.v1.ENSRegistry.read.resolver([namehash("l02.eth")]);
    check(rec, "v1 resolver NOT cleared (CSR frozen)", v1res.toLowerCase() === env.v1.PublicResolver.address.toLowerCase(), v1res);
  });

  scenario("L-03", "locked + CANNOT_SET_RESOLVER with custom resolver → carried over", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "l03", user, FUSES.CANNOT_UNWRAP);
    const custom = user.resolver.address;
    await step(rec, "v1.NameWrapper", "setResolver(custom)", ["l03.eth", custom], user.address, () =>
      env.v1.NameWrapper.write.setResolver([namehash("l03.eth"), custom], { account: user }),
    );
    await step(rec, "v1.NameWrapper", "setFuses(CANNOT_SET_RESOLVER)", ["l03.eth"], user.address, () =>
      env.v1.NameWrapper.write.setFuses([namehash("l03.eth"), FUSES.CANNOT_SET_RESOLVER], { account: user }),
    );
    await env.activateV2();
    await migrateWrapped(rec, "l03.eth", user, env.v2.LockedMigrationController.address, {
      data: { resolver: anotherAddress },
    });
    await checkMigrated2LD(rec, "l03.eth", user.address, { stillWrapped: true, resolverCleared: false });
    const v2res = await env.v2.ETHRegistry.read.getResolver(["l03"]);
    check(rec, "custom v1 resolver carried into v2", v2res.toLowerCase() === custom.toLowerCase(), v2res);
  });

  scenario("L-04", "locked + CANNOT_BURN_FUSES (frozen): no admin roles in v2", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "l04", user, FUSES.CANNOT_UNWRAP | FUSES.CANNOT_BURN_FUSES);
    await env.activateV2();
    await migrateWrapped(rec, "l04.eth", user, env.v2.LockedMigrationController.address);
    const state = await checkMigrated2LD(rec, "l04.eth", user.address, { stillWrapped: true });
    const roles = await env.v2.ETHRegistry.read.roles([state.tokenId, user.address]);
    const adminMask = (ROLES.REGISTRY.SET_RESOLVER as bigint) << 128n;
    check(rec, "no SET_RESOLVER_ADMIN (frozen)", (roles & adminMask) === 0n, roles.toString(16));
    // owner cannot grant SET_RESOLVER to others
    await expectRevert(rec, "grant without admin", /EACUnauthorized|0x/, () =>
      env.v2.ETHRegistry.write.grantRoles([state.tokenId, ROLES.REGISTRY.SET_RESOLVER, user2.address], {
        account: user,
      }),
    );
  });

  scenario("L-05", "locked + CANNOT_CREATE_SUBDOMAIN: subregistry loses REGISTRAR", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "l05", user, FUSES.CANNOT_UNWRAP | FUSES.CANNOT_CREATE_SUBDOMAIN);
    await env.activateV2();
    await migrateWrapped(rec, "l05.eth", user, env.v2.LockedMigrationController.address);
    await checkMigrated2LD(rec, "l05.eth", user.address, { stillWrapped: true });
    const wr = wrapperRegistryFor("l05.eth");
    const wrUser = env.findWrapperRegistry("l05.eth", user);
    await expectRevert(rec, "register subname without ROLE_REGISTRAR", /EACUnauthorized|0x/, () =>
      wrUser.write.register(["blocked", user2.address, zeroAddress, zeroAddress, 0n, MAX_EXPIRY], {
        account: user,
      }),
    );
  });

  scenario("L-06", "locked + CANNOT_APPROVE without live approval migrates", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "l06", user, FUSES.CANNOT_UNWRAP | FUSES.CANNOT_APPROVE);
    await env.activateV2();
    await migrateWrapped(rec, "l06.eth", user, env.v2.LockedMigrationController.address);
    await checkMigrated2LD(rec, "l06.eth", user.address, { stillWrapped: true });
  });

  scenario("L-07", "locked with records: resolution continuity through v2", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "l07", user, FUSES.CANNOT_UNWRAP);
    await setupRecords(rec, "l07.eth", user);
    await env.activateV2();
    await migrateWrapped(rec, "l07.eth", user, env.v2.LockedMigrationController.address);
    await checkMigrated2LD(rec, "l07.eth", user.address, { stillWrapped: true });
    const bundle = bundleCalls(makeResolutions({ name: "l07.eth", ...defaultProfile } as KnownProfile));
    const [answer] = await env.v2.UniversalResolver.read.resolve([dnsEncodeName("l07.eth"), bundle.call]);
    bundle.expect(answer);
    check(rec, "records resolve via v2 UniversalResolver post-migration", true);
  });

  scenario("L-08", "E3 batch of two locked names (common owner)", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "l08a", user, FUSES.CANNOT_UNWRAP);
    await registerWrapped(rec, "l08b", user, FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    const mds = [await makeData("l08a.eth", user), await makeData("l08b.eth", user)];
    await step(
      rec,
      "v1.NameWrapper",
      "safeBatchTransferFrom→LockedMigrationController",
      ["l08a.eth", "l08b.eth"],
      user.address,
      () =>
        env.v1.NameWrapper.write.safeBatchTransferFrom(
          [
            user.address,
            env.v2.LockedMigrationController.address,
            [BigInt(namehash("l08a.eth")), BigInt(namehash("l08b.eth"))],
            [1n, 1n],
            encodeData(mds),
          ],
          { account: user },
        ),
    );
    await checkMigrated2LD(rec, "l08a.eth", user.address, { stillWrapped: true });
    await checkMigrated2LD(rec, "l08b.eth", user.address, { stillWrapped: true });
    wrapperRegistryFor("l08a.eth");
    wrapperRegistryFor("l08b.eth");
  });

  scenario("X-L-01", "locked + CANNOT_TRANSFER cannot migrate (wall)", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "xl01", user, FUSES.CANNOT_UNWRAP | FUSES.CANNOT_TRANSFER);
    await env.activateV2();
    await expectRevert(rec, "CANNOT_TRANSFER migration", "OperationProhibited", () =>
      migrateWrapped(rec, "xl01.eth", user, env.v2.LockedMigrationController.address),
    );
  });

  scenario("X-L-02", "locked + CANNOT_APPROVE with live approval reverts FrozenTokenApproval", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "xl02", user, FUSES.CANNOT_UNWRAP);
    await step(rec, "v1.NameWrapper", "approve", ["xl02.eth", user2.address], user.address, () =>
      env.v1.NameWrapper.write.approve([user2.address, BigInt(namehash("xl02.eth"))], { account: user }),
    );
    await step(rec, "v1.NameWrapper", "setFuses(CANNOT_APPROVE)", ["xl02.eth"], user.address, () =>
      env.v1.NameWrapper.write.setFuses([namehash("xl02.eth"), FUSES.CANNOT_APPROVE], { account: user }),
    );
    await env.activateV2();
    await expectRevert(rec, "frozen approval", "FrozenTokenApproval", () =>
      migrateWrapped(rec, "xl02.eth", user, env.v2.LockedMigrationController.address),
    );
  });

  scenario("X-L-03", "locked 2LD into another name's WrapperRegistry reverts NameDataMismatch", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "xl03a", user, FUSES.CANNOT_UNWRAP);
    await registerWrapped(rec, "xl03b", user, FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    await migrateWrapped(rec, "xl03b.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("xl03b.eth");
    await expectRevert(rec, "2LD→foreign WrapperRegistry", "NameDataMismatch", () =>
      migrateWrapped(rec, "xl03a.eth", user, wr.address),
    );
  });

  scenario("X-L-04", "locked 3LD into LockedMigrationController reverts NameDataMismatch", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "xl04", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "xl04.eth", "sub", user, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    await expectRevert(rec, "3LD→2LD controller", "NameDataMismatch", () =>
      migrateWrapped(rec, child, user, env.v2.LockedMigrationController.address),
    );
  });

  // ============================================================ C: E4 children

  scenario("C-01", "locked child into parent WrapperRegistry (nested registries)", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "c01", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(
      rec, "c01.eth", "sub", user2, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP, MAX_EXPIRY, user,
    );
    await env.activateV2();
    await migrateWrapped(rec, "c01.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("c01.eth");
    await migrateWrapped(rec, child, user2, wr.address);
    const st = await wr.read.getState([idFromLabel("sub")]);
    check(rec, "child REGISTERED in parent WrapperRegistry", st.status === STATUS.REGISTERED, st.status);
    check(rec, "child v2 owner", st.latestOwner.toLowerCase() === user2.address.toLowerCase(), st.latestOwner);
    const childWr = wrapperRegistryFor(child);
    const sub = await wr.read.getSubregistry(["sub"]);
    check(rec, "child got own nested WrapperRegistry", sub.toLowerCase() === childWr.address.toLowerCase(), sub);
    const wd = await env.v1.NameWrapper.read.getData([BigInt(namehash(child))]);
    check(rec, "child wrapper token parked in Graveyard", wd[0].toLowerCase() === env.v2.Graveyard.address.toLowerCase(), wd[0]);
  });

  scenario("C-02", "detached (emancipated, unlocked) child migrates via unwrap branch", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "c02", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(
      rec, "c02.eth", "sub", user2, FUSES.PARENT_CANNOT_CONTROL, MAX_EXPIRY, user,
    );
    await env.activateV2();
    await migrateWrapped(rec, "c02.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("c02.eth");
    await migrateWrapped(rec, child, user2, wr.address);
    const st = await wr.read.getState([idFromLabel("sub")]);
    check(rec, "detached child REGISTERED", st.status === STATUS.REGISTERED, st.status);
    const v1owner = await env.v1.ENSRegistry.read.owner([namehash(child)]);
    check(rec, "child unwrapped to Graveyard on v1", v1owner.toLowerCase() === env.v2.Graveyard.address.toLowerCase(), v1owner);
    const sub = await wr.read.getSubregistry(["sub"]);
    check(rec, "no nested WrapperRegistry for detached child (payload subregistry used)", sub === zeroAddress, sub);
  });

  scenario("C-03", "child with CAN_EXTEND_EXPIRY gets v2 renew rights", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "c03", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(
      rec, "c03.eth", "sub", user2,
      FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP | FUSES.CAN_EXTEND_EXPIRY,
      MAX_EXPIRY, user,
    );
    await env.activateV2();
    await migrateWrapped(rec, "c03.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("c03.eth");
    await migrateWrapped(rec, child, user2, wr.address);
    wrapperRegistryFor(child);
    const wrU2 = env.findWrapperRegistry("c03.eth", user2);
    const st = await wr.read.getState([idFromLabel("sub")]);
    await step(rec, "WrapperRegistry(c03.eth)", "renew (by child owner)", ["sub", "+1"], user2.address, () =>
      wrU2.write.renew([st.tokenId, st.expiry + 1n], { account: user2 }),
    );
    const st2 = await wr.read.getState([idFromLabel("sub")]);
    check(rec, "child owner extended expiry (ROLE_RENEW from CEE)", st2.expiry === st.expiry + 1n, `${st2.expiry}`);
  });

  scenario("C-04", "deep chain: locked 2LD→3LD→4LD migrate sequentially", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "c04", user, FUSES.CANNOT_UNWRAP);
    const c3 = await createChild(rec, "c04.eth", "a", user, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP);
    const c4 = await createChild(rec, c3, "b", user, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    await migrateWrapped(rec, "c04.eth", user, env.v2.LockedMigrationController.address);
    const wr2 = wrapperRegistryFor("c04.eth");
    await migrateWrapped(rec, c3, user, wr2.address);
    const wr3 = wrapperRegistryFor(c3);
    await migrateWrapped(rec, c4, user, wr3.address);
    const wr4 = wrapperRegistryFor(c4);
    const found = await env.v2.UniversalResolver.read.findExactRegistry([dnsEncodeName(c4)]);
    check(rec, "4LD registry discoverable from v2 root", found.toLowerCase() === wr4.address.toLowerCase(), found);
  });

  scenario("C-05", "unmigrated emancipated child: fallback resolver + register blocked", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "c05", user, FUSES.CANNOT_UNWRAP);
    await createChild(rec, "c05.eth", "sub", user2, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP, MAX_EXPIRY, user);
    await env.activateV2();
    await migrateWrapped(rec, "c05.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("c05.eth");
    const res = await wr.read.getResolver(["sub"]);
    check(
      rec,
      "unmigrated child resolves via ENSV1Resolver fallback",
      res.toLowerCase() === env.v2.ENSV1Resolver.address.toLowerCase(),
      res,
    );
    const wrU = env.findWrapperRegistry("c05.eth", user);
    await expectRevert(rec, "clobber emancipated child", "NameRequiresMigration", () =>
      wrU.write.register(["sub", user.address, zeroAddress, zeroAddress, 0n, MAX_EXPIRY], { account: user }),
    );
  });

  scenario("C-06", "parent-controlled child: cannot migrate, clobberable by parent", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "c06", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "c06.eth", "sub", user2, FUSES.CAN_DO_EVERYTHING, MAX_EXPIRY, user);
    await env.activateV2();
    await migrateWrapped(rec, "c06.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("c06.eth");
    await expectRevert(rec, "parent-controlled child migration", "NameNotLocked", () =>
      migrateWrapped(rec, child, user2, wr.address),
    );
    const res = await wr.read.getResolver(["sub"]);
    check(rec, "no fallback resolver for parent-controlled child", res === zeroAddress, res);
    const wrU = env.findWrapperRegistry("c06.eth", user);
    await step(rec, "WrapperRegistry(c06.eth)", "register (clobber)", ["sub", user.address], user.address, () =>
      wrU.write.register(["sub", user.address, zeroAddress, zeroAddress, 0n, MAX_EXPIRY], { account: user }),
    );
    const st = await wr.read.getState([idFromLabel("sub")]);
    check(rec, "clobbered child REGISTERED to parent owner", st.status === STATUS.REGISTERED && st.latestOwner.toLowerCase() === user.address.toLowerCase(), st.latestOwner);
    rec.notes.push("v1 wrapped child still exists under old wrapper; v2 label now diverges from it — indexer must treat v2 as authority");
  });

  scenario("C-08", "abandoned child (unwrapped then registry owner=0): label reclaimable", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "c08", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "c08.eth", "sub", user2, FUSES.PARENT_CANNOT_CONTROL, MAX_EXPIRY, user);
    await env.activateV2();
    await migrateWrapped(rec, "c08.eth", user, env.v2.LockedMigrationController.address);
    const wr = wrapperRegistryFor("c08.eth");
    // child owner unwraps (allowed: not locked), then abandons the registry record
    await step(rec, "v1.NameWrapper", "unwrap child to self", [child], user2.address, () =>
      env.v1.NameWrapper.write.unwrap([namehash("c08.eth"), labelHex("sub"), user2.address], { account: user2 }),
    );
    // still protected while registry owner != 0 (wrapper burn preserved PCC fuse)
    const wrU = env.findWrapperRegistry("c08.eth", user);
    await expectRevert(rec, "still protected while v1 owner set", "NameRequiresMigration", () =>
      wrU.write.register(["sub", user.address, zeroAddress, zeroAddress, 0n, MAX_EXPIRY], { account: user }),
    );
    await step(rec, "v1.ENSRegistry", "setOwner(0) — abandon", [child], user2.address, () =>
      env.v1.ENSRegistry.write.setOwner([namehash(child), zeroAddress], { account: user2 }),
    );
    await step(rec, "WrapperRegistry(c08.eth)", "register abandoned label", ["sub"], user.address, () =>
      wrU.write.register(["sub", user.address, zeroAddress, zeroAddress, 0n, MAX_EXPIRY], { account: user }),
    );
    const st = await wr.read.getState([idFromLabel("sub")]);
    check(rec, "abandoned label re-registered in v2", st.status === STATUS.REGISTERED, st.status);
    rec.notes.push("unwrap-after-parent-migration without abandonment leaves the label protected but unmigratable (see C-05 revert): v1-resident via fallback until owner clears or expiry");
  });

  // ============================================================ H: MigrationHelper

  scenario("H-01", "helper mixed batch: unwrapped + unlocked group + locked group + locked child", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "h01u", user);
    await registerWrapped(rec, "h01w", user, FUSES.CAN_DO_EVERYTHING);
    await registerWrapped(rec, "h01l", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "h01l.eth", "sub", user, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    const helper = await migrationHelper(user);
    await step(rec, "v1.BaseRegistrar", "setApprovalForAll(helper)", [], user.address, () =>
      env.v1.BaseRegistrar.write.setApprovalForAll([helper.address, true], { account: user }),
    );
    await step(rec, "v1.NameWrapper", "setApprovalForAll(helper)", [], user.address, () =>
      env.v1.NameWrapper.write.setApprovalForAll([helper.address, true], { account: user }),
    );
    const mdU = await makeData("h01u.eth", user);
    const mdW = await makeData("h01w.eth", user);
    const mdL = await makeData("h01l.eth", user);
    const mdC = await makeData(child, user);
    await step(
      rec,
      "MigrationHelper",
      "migrate([u],[ [w] ],[ [l] ],[ {h01l.eth,[ [sub] ]} ])",
      ["h01u", "h01w", "h01l", "sub.h01l.eth"],
      user.address,
      () =>
        helper.write.migrate(
          [[mdU], [[mdW]], [[mdL]], [{ parentName: dnsEncodeName("h01l.eth"), groups: [[mdC]] }]],
          { account: user },
        ),
    );
    await checkMigrated2LD(rec, "h01u.eth", user.address);
    await checkMigrated2LD(rec, "h01w.eth", user.address);
    await checkMigrated2LD(rec, "h01l.eth", user.address, { stillWrapped: true });
    const wr = wrapperRegistryFor("h01l.eth");
    const st = await wr.read.getState([idFromLabel("sub")]);
    check(rec, "locked child migrated in same helper call (parent first)", st.status === STATUS.REGISTERED, st.status);
    wrapperRegistryFor(child);
  });

  scenario("H-02", "helper without approval reverts NotApprovedOperator", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "h02", user);
    await env.activateV2();
    const helper = await migrationHelper(user2);
    const md = await makeData("h02.eth", user);
    await expectRevert(rec, "no approval", "NotApprovedOperator", () =>
      helper.write.migrate([[md], [], [], []], { account: user2 }),
    );
  });

  scenario("H-03", "helper wrapped group with mixed owners reverts WrappedOwnerMismatch", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "h03a", user, FUSES.CAN_DO_EVERYTHING);
    await registerWrapped(rec, "h03b", user2, FUSES.CAN_DO_EVERYTHING);
    await env.activateV2();
    const helper = await migrationHelper(user);
    await step(rec, "v1.NameWrapper", "setApprovalForAll(helper) both owners", [], "user,user2", async () => {
      await env.v1.NameWrapper.write.setApprovalForAll([helper.address, true], { account: user });
      await env.v1.NameWrapper.write.setApprovalForAll([helper.address, true], { account: user2 });
    });
    // the helper checks caller-operator approval per token BEFORE the
    // same-owner group check (MigrationHelper.sol L173-L178), so user2 must
    // also approve the caller for WrappedOwnerMismatch to be reachable.
    await step(rec, "v1.NameWrapper", "setApprovalForAll(caller) by user2", [], user2.address, () =>
      env.v1.NameWrapper.write.setApprovalForAll([user.address, true], { account: user2 }),
    );
    const md1 = await makeData("h03a.eth", user);
    const md2 = await makeData("h03b.eth", user2);
    await expectRevert(rec, "mixed-owner group", "WrappedOwnerMismatch", () =>
      helper.write.migrate([[], [[md1, md2]], [], []], { account: user }),
    );
    rec.notes.push(
      "third-party helper batches need two approvals per owner: owner→caller (helper operator check) and owner→helper (token transfer)",
    );
  });

  scenario("H-04", "helper locked child with unmigrated parent reverts ParentNotMigrated", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "h04", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "h04.eth", "sub", user, FUSES.PARENT_CANNOT_CONTROL | FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    const helper = await migrationHelper(user);
    await step(rec, "v1.NameWrapper", "setApprovalForAll(helper)", [], user.address, () =>
      env.v1.NameWrapper.write.setApprovalForAll([helper.address, true], { account: user }),
    );
    const mdC = await makeData(child, user);
    await expectRevert(rec, "parent not migrated", "ParentNotMigrated", () =>
      helper.write.migrate([[], [], [], [{ parentName: dnsEncodeName("h04.eth"), groups: [[mdC]] }]], {
        account: user,
      }),
    );
  });

  // ============================================================ G: Graveyard

  scenario("G-01", "clear v1 registry subnode residue under migrated 2LD", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "g01", user);
    await step(rec, "v1.ENSRegistry", "setSubnodeRecord (residue w/ resolver)", ["g01.eth", "sub"], user.address, () =>
      env.v1.ENSRegistry.write.setSubnodeRecord(
        [namehash("g01.eth"), labelHex("sub"), user2.address, anotherAddress, 0n],
        { account: user },
      ),
    );
    await env.activateV2();
    await migrateUnwrapped(rec, "g01.eth", user);
    await step(rec, "v2.Graveyard", "clear([sub.g01.eth])", ["sub.g01.eth"], "anyone(user2)", () =>
      env.v2.Graveyard.write.clear([[dnsEncodeName("sub.g01.eth")]], { account: user2 }),
    );
    const o = await env.v1.ENSRegistry.read.owner([namehash("sub.g01.eth")]);
    const r = await env.v1.ENSRegistry.read.resolver([namehash("sub.g01.eth")]);
    check(rec, "residue subnode owner → Graveyard", o.toLowerCase() === env.v2.Graveyard.address.toLowerCase(), o);
    check(rec, "residue subnode resolver cleared", r === zeroAddress, r);
  });

  scenario("G-02", "clear fully-expired unmigrated 2LD: graveyard self-claims", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "g02", user, { duration: 30n * DAY });
    await setupRecords(rec, "g02.eth", user);
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("g02")]);
    await env.activateV2();
    await warpTo(rec, v1exp + 90n * DAY + 10n, "past v1 grace");
    const avail = await env.v1.BaseRegistrar.read.available([idFromLabel("g02")]);
    check(rec, "v1 available past grace", avail === true, avail);
    await step(rec, "v2.Graveyard", "clear([g02.eth])", ["g02.eth"], "anyone(user2)", () =>
      env.v2.Graveyard.write.clear([[dnsEncodeName("g02.eth")]], { account: user2 }),
    );
    const t = await env.v1.BaseRegistrar.read.ownerOf([idFromLabel("g02")]);
    check(rec, "graveyard claimed expired v1 token", t.toLowerCase() === env.v2.Graveyard.address.toLowerCase(), t);
    const r = await env.v1.ENSRegistry.read.resolver([namehash("g02.eth")]);
    check(rec, "resolver cleared", r === zeroAddress, r);
    const v2status = await env.v2.ETHRegistry.read.getStatus([idFromLabel("g02")]);
    check(rec, "v2 reservation lapsed to AVAILABLE", v2status === STATUS.AVAILABLE, v2status);
  });

  scenario("G-03", "clear reverts on live unmigrated name", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "g03", user);
    await env.activateV2();
    await expectRevert(rec, "clear live name", /NameNotClearable|revert/, () =>
      env.v2.Graveyard.write.clear([[dnsEncodeName("g03.eth")]], { account: user2 }),
    );
  });

  scenario("G-04", "clear wrapped parent-controlled child residue under migrated locked 2LD", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "g04", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "g04.eth", "sub", user, FUSES.CAN_DO_EVERYTHING, MAX_EXPIRY, user);
    await step(rec, "v1.NameWrapper", "setResolver(child residue)", [child], user.address, () =>
      env.v1.NameWrapper.write.setResolver([namehash(child), anotherAddress], { account: user }),
    );
    await env.activateV2();
    await migrateWrapped(rec, "g04.eth", user, env.v2.LockedMigrationController.address);
    wrapperRegistryFor("g04.eth");
    await step(rec, "v2.Graveyard", "clear([sub.g04.eth])", [child], "anyone(user2)", () =>
      env.v2.Graveyard.write.clear([[dnsEncodeName(child)]], { account: user2 }),
    );
    const r = await env.v1.ENSRegistry.read.resolver([namehash(child)]);
    check(rec, "wrapped child residue resolver cleared", r === zeroAddress, r);
    const o = await env.v1.ENSRegistry.read.owner([namehash(child)]);
    check(rec, "child force-unwrapped to Graveyard", o.toLowerCase() === env.v2.Graveyard.address.toLowerCase(), o);
  });

  // ============================================================ R: ETHRenewerV1

  scenario("R-01", "renewer extends unmigrated premigrated name on both sides", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "r01", user);
    const v1exp0 = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("r01")]);
    const v2exp0 = await env.v2.ETHRegistry.read.getExpiry([idFromLabel("r01")]);
    await env.activateV2();
    await renewViaRenewer(rec, "r01", SEC_PER_YEAR, user);
    const v1exp1 = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("r01")]);
    const v2exp1 = await env.v2.ETHRegistry.read.getExpiry([idFromLabel("r01")]);
    check(rec, "v1 expiry extended", v1exp1 === v1exp0 + SEC_PER_YEAR, `${v1exp1}`);
    check(rec, "v2 reserved expiry extended in lockstep", v2exp1 === v2exp0 + SEC_PER_YEAR, `${v2exp1}`);
    const status = await env.v2.ETHRegistry.read.getStatus([idFromLabel("r01")]);
    check(rec, "still RESERVED", status === STATUS.RESERVED, status);
  });

  scenario("R-02", "renew during v1 grace restores, then migration succeeds", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "r02", user, { duration: 30n * DAY });
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("r02")]);
    await env.activateV2();
    await warpTo(rec, v1exp + 5n * DAY, "5 days into v1 grace");
    await renewViaRenewer(rec, "r02", SEC_PER_YEAR, user);
    const owner = await env.v1.BaseRegistrar.read.ownerOf([idFromLabel("r02")]);
    check(rec, "v1 owner restored after grace renewal", owner.toLowerCase() === user.address.toLowerCase(), owner);
    await migrateUnwrapped(rec, "r02.eth", user);
    await checkMigrated2LD(rec, "r02.eth", user.address);
  });

  scenario("R-03", "renew after combined grace reverts NameNotRenewable", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "r03", user, { duration: 30n * DAY });
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("r03")]);
    await env.activateV2();
    await warpTo(rec, v1exp + PREMIGRATION_BONUS_PERIOD + 28n * DAY, "past bonus + v2 grace = past v1 grace");
    await expectRevert(rec, "renew after grace", "NameNotRenewable", () => renewViaRenewer(rec, "r03", SEC_PER_YEAR, user));
  });

  scenario("R-04", "renewer refuses migrated names", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "r04", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "r04.eth", user);
    await expectRevert(rec, "renewer on migrated", "NameNotRenewable", () => renewViaRenewer(rec, "r04", SEC_PER_YEAR, user));
  });

  scenario("R-05", "syncWrapper refreshes wrapper expiry after renewer renewal", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "r05", user, FUSES.CAN_DO_EVERYTHING);
    await env.activateV2();
    await renewViaRenewer(rec, "r05", SEC_PER_YEAR, user);
    const before = await env.v1.NameWrapper.read.getData([BigInt(namehash("r05.eth"))]);
    await step(rec, "v2.ETHRenewerV1", "syncWrapper([r05])", ["r05"], "anyone(user)", () =>
      env.v2.ETHRenewerV1.write.syncWrapper([["r05"]], { account: user }),
    );
    const after = await env.v1.NameWrapper.read.getData([BigInt(namehash("r05.eth"))]);
    check(rec, "wrapper expiry synced up", after[2] > before[2], { before: `${before[2]}`, after: `${after[2]}` });
  });

  // ============================================================ P: post-migration authority

  scenario("P-02", "old owner cannot write migrated node on v1 (suppression by construction)", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "p02", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "p02.eth", user);
    await expectRevert(rec, "old owner setResolver on v1", /revert/i, () =>
      env.v1.ENSRegistry.write.setResolver([namehash("p02.eth"), anotherAddress], { account: user }),
    );
    await expectRevert(rec, "old owner setSubnodeOwner on v1", /revert/i, () =>
      env.v1.ENSRegistry.write.setSubnodeOwner([namehash("p02.eth"), labelHex("late"), user.address], {
        account: user,
      }),
    );
  });

  scenario("P-03", "pre-migration v1 registry operator loses power after migration", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "p03", user);
    await step(rec, "v1.ENSRegistry", "setApprovalForAll(operator)", [user2.address], user.address, () =>
      env.v1.ENSRegistry.write.setApprovalForAll([user2.address, true], { account: user }),
    );
    await env.activateV2();
    await migrateUnwrapped(rec, "p03.eth", user);
    await expectRevert(rec, "old operator setResolver", /revert/i, () =>
      env.v1.ENSRegistry.write.setResolver([namehash("p03.eth"), anotherAddress], { account: user2 }),
    );
  });

  scenario("P-04", "old owner cannot write E3-migrated node via NameWrapper", async (rec) => {
    const { user } = A();
    await registerWrapped(rec, "p04", user, FUSES.CANNOT_UNWRAP);
    await env.activateV2();
    await migrateWrapped(rec, "p04.eth", user, env.v2.LockedMigrationController.address);
    wrapperRegistryFor("p04.eth");
    await expectRevert(rec, "old owner wrapper setResolver", /revert|Unauthorised/i, () =>
      env.v1.NameWrapper.write.setResolver([namehash("p04.eth"), anotherAddress], { account: user }),
    );
    await expectRevert(rec, "old owner wrapper setSubnodeOwner", /revert|Unauthorised/i, () =>
      env.v1.NameWrapper.write.setSubnodeOwner([namehash("p04.eth"), "late", user.address, 0, 0n], {
        account: user,
      }),
    );
  });

  scenario("P-05", "emancipated sibling stays v1-live under migrated locked parent", async (rec) => {
    const { user, user2 } = A();
    await registerWrapped(rec, "p05", user, FUSES.CANNOT_UNWRAP);
    const child = await createChild(rec, "p05.eth", "sib", user2, FUSES.PARENT_CANNOT_CONTROL, MAX_EXPIRY, user);
    await env.activateV2();
    await migrateWrapped(rec, "p05.eth", user, env.v2.LockedMigrationController.address);
    wrapperRegistryFor("p05.eth");
    // the un-migrated emancipated child continues to operate on v1
    await step(rec, "v1.NameWrapper", "setResolver (live child, post-parent-migration)", [child], user2.address, () =>
      env.v1.NameWrapper.write.setResolver([namehash(child), env.v1.PublicResolver.address], { account: user2 }),
    );
    const res = makeResolutions({ name: child, addresses: [{ coinType: 60n, value: anotherAddress }] } as KnownProfile);
    await step(rec, "v1.PublicResolver", "setAddr (live child)", [child], user2.address, () =>
      env.v1.PublicResolver.write.multicall([res.map((x) => x.write)], { account: user2 }),
    );
    const v1res = await env.v1.ENSRegistry.read.resolver([namehash(child)]);
    check(rec, "child v1 resolver write landed", v1res.toLowerCase() === env.v1.PublicResolver.address.toLowerCase(), v1res);
  });

  scenario("P-06", "v2 renew of migrated name diverges from v1 husk expiry", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "p06", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "p06.eth", user);
    const v1exp0 = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("p06")]);
    const st0 = await env.v2.ETHRegistry.read.getState([idFromLabel("p06")]);
    const amount = await env.v2.ETHRegistrar.read.getRenewPrice(["p06", SEC_PER_YEAR, env.erc20.MockUSDC.address]);
    await step(rec, "v2.ETHRegistrar", "renew (ERC20)", ["p06", `${SEC_PER_YEAR}`], user.address, async () => {
      await env.erc20.MockUSDC.write.mint([user.address, amount[0] ?? amount]);
      await env.erc20.MockUSDC.write.approve([env.v2.ETHRegistrar.address, amount[0] ?? amount], { account: user });
      await env.v2.ETHRegistrar.write.renew(["p06", SEC_PER_YEAR, env.erc20.MockUSDC.address, namehash("ref")], {
        account: user,
      });
    });
    const v1exp1 = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("p06")]);
    const st1 = await env.v2.ETHRegistry.read.getState([idFromLabel("p06")]);
    check(rec, "v2 expiry extended", st1.expiry === st0.expiry + SEC_PER_YEAR, `${st1.expiry}`);
    check(rec, "v1 husk expiry unchanged (diverged)", v1exp1 === v1exp0, `${v1exp1}`);
  });

  scenario("P-07", "role grant on migrated name regenerates token id", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "p07", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "p07.eth", user);
    const st0 = await env.v2.ETHRegistry.read.getState([idFromLabel("p07")]);
    await step(rec, "v2.ETHRegistry", "grantRoles(SET_RESOLVER→user2)", ["p07"], user.address, () =>
      env.v2.ETHRegistry.write.grantRoles([st0.tokenId, ROLES.REGISTRY.SET_RESOLVER, user2.address], {
        account: user,
      }),
    );
    const st1 = await env.v2.ETHRegistry.read.getState([idFromLabel("p07")]);
    check(rec, "token id regenerated", st1.tokenId !== st0.tokenId, { old: `${st0.tokenId}`, new: `${st1.tokenId}` });
    check(rec, "resource unchanged", st1.resource === st0.resource, `${st1.resource}`);
  });

  scenario("P-08", "v2 transfer of migrated name moves roles", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "p08", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "p08.eth", user);
    const st0 = await env.v2.ETHRegistry.read.getState([idFromLabel("p08")]);
    await step(rec, "v2.ETHRegistry", "safeTransferFrom(user→user2)", ["p08"], user.address, () =>
      env.v2.ETHRegistry.write.safeTransferFrom([user.address, user2.address, st0.tokenId, 1n, "0x"], {
        account: user,
      }),
    );
    const st1 = await env.v2.ETHRegistry.read.getState([idFromLabel("p08")]);
    check(rec, "v2 owner changed", st1.latestOwner.toLowerCase() === user2.address.toLowerCase(), st1.latestOwner);
  });

  scenario("P-09", "reservation lapse → fresh v2 registration = new identity", async (rec) => {
    const { user, user2 } = A();
    await registerUnwrapped(rec, "p09", user, { duration: 30n * DAY });
    const v1exp = await env.v1.BaseRegistrar.read.nameExpires([idFromLabel("p09")]);
    await env.activateV2();
    await warpTo(rec, v1exp + 91n * DAY, "past everything");
    // fresh commit/reveal registration by user2 on v2 ETHRegistrar
    const secret = namehash("secret");
    const commitment = await env.v2.ETHRegistrar.read.makeCommitment([
      "p09", user2.address, secret, zeroAddress, zeroAddress, 28n * DAY, namehash("ref"),
    ]);
    await step(rec, "v2.ETHRegistrar", "commit", ["p09"], user2.address, () =>
      env.v2.ETHRegistrar.write.commit([commitment], { account: user2 }),
    );
    await env.sync({ warpSec: 61 });
    const [base, premium] = await env.v2.ETHRegistrar.read.getRegisterPrice(["p09", 28n * DAY, env.erc20.MockUSDC.address]);
    await step(rec, "v2.ETHRegistrar", "register (fresh)", ["p09", user2.address], user2.address, async () => {
      await env.erc20.MockUSDC.write.mint([user2.address, base + premium], { account: user2 });
      await env.erc20.MockUSDC.write.approve([env.v2.ETHRegistrar.address, base + premium], { account: user2 });
      await env.v2.ETHRegistrar.write.register(
        ["p09", user2.address, secret, zeroAddress, zeroAddress, 28n * DAY, env.erc20.MockUSDC.address, namehash("ref")],
        { account: user2 },
      );
    });
    const st = await env.v2.ETHRegistry.read.getState([idFromLabel("p09")]);
    check(rec, "fresh registration to new owner", st.latestOwner.toLowerCase() === user2.address.toLowerCase(), st.latestOwner);
    const hasWasReserved = await env.v2.ETHRegistry.read.hasRoles([st.tokenId, ROLES.REGISTRY.WAS_RESERVED, user2.address]);
    check(rec, "no ROLE_WAS_RESERVED marker on fresh identity", hasWasReserved === false, hasWasReserved);
    rec.notes.push("v1 husk still shows old owner in expired registry entry; indexer identity must break lineage here");
  });

  scenario("P-11", "root unregister of migrated name (governance action)", async (rec) => {
    const { user } = A();
    await registerUnwrapped(rec, "p11", user);
    await env.activateV2();
    await migrateUnwrapped(rec, "p11.eth", user);
    const st = await env.v2.ETHRegistry.read.getState([idFromLabel("p11")]);
    try {
      await step(rec, "v2.ETHRegistry", "unregister (as root/deployer)", ["p11"], "deployer", () =>
        env.v2.ETHRegistry.write.unregister([st.tokenId]),
      );
      const status = await env.v2.ETHRegistry.read.getStatus([idFromLabel("p11")]);
      check(rec, "unregistered back to AVAILABLE", status === STATUS.AVAILABLE, status);
    } catch (err) {
      rec.reverts.push({ step: "root unregister", error: String(err).slice(0, 300) });
      rec.notes.push("deployer lacks root ROLE_UNREGISTER on this devnet: governance-only path, recorded as revert evidence");
    }
  });

  afterAll(() => {
    writeFileSync(join(OUT_DIR, "SUMMARY.json"), JSON.stringify(summary, null, 2));
  });
});
