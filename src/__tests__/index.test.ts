import Queue from 'bull';

import { reloadConfig, BullConfig } from '..';

describe('bull-reloadable-config', () => {
  const VERSION_0_0_4 = '0.0.4';
  let queueName = 'testQueueName';
  const queueOptions = {};
  const extraQueueName = 'extraQueueName';
  let queue = new Queue(queueName);
  const extraQueue = new Queue(extraQueueName);

  beforeEach(async () => {
    await queue.close();
    queueName = `${Math.random() * 10000}random`;
    // eslint-disable-next-line require-atomic-updates
    queue = new Queue(queueName);
  });

  afterEach(async () => {
    await queue.clean(0);
    await queue.empty();
    await extraQueue.empty();
  });

  afterAll(async () => {
    await queue.close();
    await extraQueue.close();
  });

  it('should accept createClient as an option', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ];
    const createClient = jest.fn().mockReturnValue(queue.client);
    await reloadConfig(queueName, { createClient }, configs);
    const job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: VERSION_0_0_4, ...extraData });
    expect(createClient).toHaveBeenCalledWith('client', expect.any(Object));
  });

  it('throws if a job as no options', async () => {
    const configs: BullConfig[] = [
      // eslint-disable-next-line @typescript-eslint/ban-ts-ignore
      //@ts-ignore
      { data: { _version: VERSION_0_0_4 } },
    ];
    await expect(
      reloadConfig(queueName, queueOptions, configs),
    ).rejects.toMatchSnapshot();
  });
  it('throws if a job as no _id', async () => {
    const configs: BullConfig[] = [
      // eslint-disable-next-line @typescript-eslint/ban-ts-ignore
      //@ts-ignore
      { opts: {}, data: { _version: VERSION_0_0_4 } },
    ];
    await expect(
      reloadConfig(queueName, queueOptions, configs),
    ).rejects.toMatchSnapshot();
  });

  it('throws if two jobs has the same ids', async () => {
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4 },
        opts: { jobId: 'id' },
      },
      {
        data: { _version: VERSION_0_0_4 },
        opts: { jobId: 'id' },
      },
    ];
    await expect(
      reloadConfig(queueName, queueOptions, configs),
    ).rejects.toMatchSnapshot();
  });

  it('creates a non existing job', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    const job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: VERSION_0_0_4, ...extraData });
  });

  it('transmits correctly the job options', async () => {
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4 },
        opts: { jobId, attempts: 5, delay: 8 },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    const job = await queue.getJob(jobId);
    expect(job!.opts).toMatchObject({ attempts: 5, delay: 8 });
  });

  it('transmits correctly the job name', async () => {
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        name: 'Heisenberg',
        data: { _version: VERSION_0_0_4 },
        opts: { jobId, attempts: 5, delay: 8 },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    const job = await queue.getJob(jobId);
    expect(job!.name).toBe('Heisenberg');
  });

  it('recreate a repeatable job', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId, repeat: { every: 5 } },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = (await queue.getRepeatableJobs())[0];
    expect(job).toEqual(
      expect.objectContaining({
        id: 'myjobid',
        every: 5,
      }),
    );

    await reloadConfig(queueName, queueOptions, [
      {
        data: { _version: '0.0.5', ...extraData },
        opts: { jobId, repeat: { every: 8 } },
      },
    ]);
    job = (await queue.getRepeatableJobs())[0];
    expect(job).toEqual(
      expect.objectContaining({
        id: 'myjobid',
        every: 8,
      }),
    );
    // job = await queue.getJob(jobId);
    // expect(job!.data).toEqual({ _version: '0.0.5', ...extraData });
    // expect(job!.opts).toMatchObject({ attempts: 5, delay: 8 });
  });

  it('recreate a job in the higher version with changes', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: VERSION_0_0_4, ...extraData });

    await reloadConfig(queueName, queueOptions, [
      {
        data: { _version: '0.0.5', ...extraData },
        opts: { jobId, attempts: 5, delay: 8 },
      },
    ]);
    job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: '0.0.5', ...extraData });
    expect(job!.opts).toMatchObject({ attempts: 5, delay: 8 });
  });

  it('always recreate a job if no version', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ ...extraData });

    await reloadConfig(queueName, queueOptions, [
      {
        data: { ...extraData },
        opts: { jobId, attempts: 5, delay: 8 },
      },
    ]);
    job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ ...extraData });
    expect(job!.opts).toMatchObject({ attempts: 5, delay: 8 });
  });

  it('accept array as data', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: [extraData],
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    const job = await queue.getJob(jobId);
    expect(job!.data).toEqual([extraData]);
  });

  it('does not recreate a job in the lower version', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: '0.0.5', ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: '0.0.5', ...extraData });

    await reloadConfig(queueName, queueOptions, [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ]);
    job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: '0.0.5', ...extraData });
  });

  it('can recreate a job in the lower version if force is true', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: '0.0.5', ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: '0.0.5', ...extraData });

    await reloadConfig(queueName, queueOptions, [
      {
        force: true,
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ]);
    job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: VERSION_0_0_4, ...extraData });
  });

  it('remove jobs not in the config only if deleteExtraJobs is true', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const otherJobId = 'otherJobId';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: VERSION_0_0_4, ...extraData });

    await reloadConfig(
      queueName,
      queueOptions,
      [
        {
          data: { _version: VERSION_0_0_4, ...extraData },
          opts: { jobId: otherJobId },
        },
      ],
      false,
    );
    job = await queue.getJob(jobId);
    expect(job).toBeTruthy();

    await reloadConfig(
      queueName,
      queueOptions,
      [
        {
          data: { _version: VERSION_0_0_4, ...extraData },
          opts: { jobId: otherJobId },
        },
      ],
      true,
    );
    job = await queue.getJob(jobId);
    expect(job).toBeFalsy();
  });

  it('can create huge job array in a reasonable time', async () => {
    jest.setTimeout(15_000);
    const count = 10_000;
    let configs: BullConfig[] = [];
    for (let i = 0; i < count / 2; i += 1) {
      configs.push({
        data: { _version: VERSION_0_0_4 },
        opts: { jobId: `i${i}` },
      });
    }
    await reloadConfig(queueName, queueOptions, configs);
    configs = [
      {
        data: { _version: VERSION_0_0_4 },
        opts: { jobId: 'd1', delay: 3600 },
      },
      {
        data: { _version: VERSION_0_0_4 },
        opts: { jobId: 'd2', delay: 8 },
      },
    ];
    for (let i = 0; i < count; i += 1) {
      configs.push({
        data: { _version: '0.0.5' },
        opts: { jobId: `i${i}` },
      });
    }
    await reloadConfig(queueName, queueOptions, configs);

    const jobCount = await queue.getJobCounts();
    expect(jobCount).toEqual({
      active: 0,
      completed: 0,
      delayed: 2,
      failed: 0,
      paused: 0,
      waiting: count,
    });
  });

  it('refuses non semver version', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: 'thisisbadversion', ...extraData },
        opts: { jobId },
      },
    ];
    await expect(
      reloadConfig(queueName, queueOptions, configs),
    ).rejects.toMatchSnapshot();
  });

  it('does not remove and add a job that did not change', async () => {
    const extraData = { a: 'g' };
    const jobId = 'myjobid';
    const configs: BullConfig[] = [
      {
        data: { _version: VERSION_0_0_4, ...extraData },
        opts: { jobId },
      },
    ];
    await reloadConfig(queueName, queueOptions, configs);
    let job = await queue.getJob(jobId);
    const previousTimestamp = job!.timestamp;
    expect(job!.data).toEqual({ _version: VERSION_0_0_4, ...extraData });

    await reloadConfig(queueName, queueOptions, [
      {
        data: { _version: '0.0.5', ...extraData },
        opts: { jobId },
      },
    ]);
    job = await queue.getJob(jobId);
    expect(job!.data).toEqual({ _version: '0.0.5', ...extraData });
    expect(job!.timestamp).toBe(previousTimestamp);
  });
});
