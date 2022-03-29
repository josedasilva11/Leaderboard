import 'isomorphic-unfetch';

import Redis from 'ioredis';
import {
  DimensionDefinition,
  Leaderboard,
  LeaderboardMatrix,
  MatrixEntry,
  PeriodicLeaderboard,
} from 'redis-rank';

import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createClient } from '@urql/core';
import Big from 'big.js';
import { Cron, CronExpression } from '@nestjs/schedule';

const APIURL =
  'https://api.thegraph.com/subgraphs/name/pancakeswap/prediction-v2';

const graphql = createClient({
  url: APIURL,
  fetchOptions: {},
});

interface Bet {
  user: {
    id: string;
  };
  amount: string;
  position: string;
}

@Injectable()
export class AppService implements OnApplicationBootstrap {
  static currentRoundKey = 'currentRound';

  client: Redis.Redis;

  leaderboard: LeaderboardMatrix;

  constructor(private readonly configService: ConfigService) {}

  onApplicationBootstrap() {
    this.client = new Redis(this.configService.get('REDIS_URL'));
    this.leaderboard = new LeaderboardMatrix(this.client, 'leaderboard', {
      dimensions: [
        { name: 'world' },
        { name: 'best-month', cycle: 'monthly' },
        { name: 'best-week', cycle: 'weekly' },
        { name: 'best-day', cycle: 'daily' },
        { name: 'best-hour', cycle: 'hourly' },
      ],
      features: [
        {
          name: 'amount_played',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'total_bet',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'winnings',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'bnb_won',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'bnb_won_even',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'losses',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'winnings_even_money',
          options: {
            updatePolicy: 'aggregate',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'winrate',
          options: {
            updatePolicy: 'replace',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
        {
          name: 'avg_bet',
          options: {
            updatePolicy: 'replace',
            sortPolicy: 'high-to-low',
            limitTopN: 500,
          },
        },
      ],
    });

    !this.isReadOnly() && this.populateLeaderboard();
  }

  isReadOnly() {
    return this.configService.get('READ_ONLY');
  }

  async getLeaderboard(dimension: string, feature: string) {
    const res = await this.leaderboard.top(dimension, feature, 500);
    return res.map((entry) => ({ address: entry.id, scores: entry.scores }));
  }

  private graphQuery(query: string, variables: any) {
    return graphql.query(query, variables).toPromise();
  }

  /**
   * Fetch the current round from Redis or fallback to the latest round
   * @returns {Promise<number>}
   */
  private async getCurrentRound(): Promise<number> {
    const currentRound = await this.client.get(AppService.currentRoundKey);

    if (currentRound) {
      return parseInt(currentRound, 10);
    }

    const lastRounds = await this.fetchLatestRound();

    if (lastRounds?.data?.rounds?.length > 1) {
      const round = lastRounds.data.rounds[1];
      await this.setCurrentRound(round.epoch);
      return parseInt(round.epoch, 10);
    }

    throw new Error('No rounds found');
  }

  /**
   * Set the current round in Redis
   * @param epoch
   * @returns
   */
  private async setCurrentRound(epoch: number) {
    Logger.verbose('Next round will be: ' + epoch);
    return this.client.set(AppService.currentRoundKey, epoch);
  }

  async fetchLatestRound() {
    const query = `
      query Rounds($orderBy: Round_orderBy, $orderDir: OrderDirection, $first: Int!, $where: Round_filter) {
        rounds(orderBy: $orderBy, orderDirection: $orderDir, first: $first, where: $where) {
          epoch
          closeBlock
        }
      }
    `;

    const variables = {
      orderBy: 'epoch',
      orderDir: 'desc',
      first: 10,
      where: {
        closeBlock_gt: 0,
      },
    };

    return this.graphQuery(query, variables);
  }

  async fetchRound(round: number) {
    const query = `
      query GetRoundBets($ID: Int!, $skip: Int!) {
        round(id: $ID) {
          id
          position
          bets(first: 1000, skip: $skip) {
            user {
              id
            }
            amount
            position
          }
          bearBets
          bullBets
          totalBets
          bullAmount
          totalAmount
          bearAmount
        }
      }    
    `;

    const variables = {
      ID: round,
      skip: 0,
    };

    return this.graphQuery(query, variables);
  }

  @Cron(CronExpression.EVERY_10_SECONDS)
  async populateLeaderboard() {
    try {
      if (this.isReadOnly()) {
        return;
      }

      await this.deleteOld();
      const currentRound: number = await this.getCurrentRound();
      Logger.verbose(`Current round: ${currentRound}`);
      const roundData = await this.fetchRound(currentRound);

      if (roundData.data) {
        const { round } = roundData.data;

        if (!round.position) {
          throw new Error('Round not ready to fetch');
        }

        const bullPayout = new Big(round.totalAmount).div(round.bullAmount);
        const bearPayout = new Big(round.totalAmount).div(round.bearAmount);

        console.log(`Bull payout: ${bullPayout}`);
        console.log('Bear payout: ' + bearPayout);

        // Optimistic update
        await this.setCurrentRound(currentRound + 1);

        // Update linear metrics
        await this.leaderboard.update(
          (round.bets as Bet[]).map((bet) => {
            return {
              id: bet.user.id,
              values: {
                amount_played: 1,
                total_bet: new Big(bet.amount).round(7, 0).toNumber(),
                winnings: bet.position === round.position ? 1 : 0,
                losses: bet.position !== round.position ? 1 : 0,
                bnb_won:
                  bet.position === round.position
                    ? (round.position === 'Bull' ? bullPayout : bearPayout)
                        .times(bet.amount)
                        .times(1 - 0.03)
                        .minus(bet.amount)
                        .round(7, 0)
                        .toNumber()
                    : 0,
                bnb_won_even:
                  bet.position === round.position
                    ? (round.position === 'Bull' ? bullPayout : bearPayout)
                        .times(1)
                        .times(1 - 0.03)
                        .minus(1)
                        .round(7, 0)
                        .toNumber()
                    : 0,
              },
            };
          }),
        );

        await Promise.all(
          this.leaderboard.options.dimensions.map(
            async (dimension: DimensionDefinition) => {
              const updates = await Promise.all(
                (round.bets as Bet[]).map(async (bet) => {
                  const lbWinnings = this.leaderboard.getRawLeaderboard(
                    dimension.name,
                    'winnings',
                  );

                  const lbTotalBet = this.leaderboard.getRawLeaderboard(
                    dimension.name,
                    'total_bet',
                  );

                  const lbAmountPlayed = this.leaderboard.getRawLeaderboard(
                    dimension.name,
                    'amount_played',
                  );

                  const prevDataWinnings =
                    dimension.name === 'world'
                      ? (await (lbWinnings as Leaderboard).find(bet.user.id))
                          ?.score || 0
                      : (
                          await (lbWinnings as PeriodicLeaderboard)
                            .getLeaderboardNow()
                            .find(bet.user.id)
                        )?.score || 0;

                  const prevDataTotalBet =
                    dimension.name === 'world'
                      ? (await (lbWinnings as Leaderboard).find(bet.user.id))
                          ?.score || 0
                      : (
                          await (lbTotalBet as PeriodicLeaderboard)
                            .getLeaderboardNow()
                            .find(bet.user.id)
                        )?.score || 0;

                  const prevAmountPlayed =
                    dimension.name === 'world'
                      ? (await (lbWinnings as Leaderboard).find(bet.user.id))
                          ?.score || 0
                      : (
                          await (lbAmountPlayed as PeriodicLeaderboard)
                            .getLeaderboardNow()
                            .find(bet.user.id)
                        )?.score || 0;

                  return {
                    id: bet.user.id,
                    values: {
                      winrate: new Big(prevDataWinnings)
                        .div(prevAmountPlayed || 1)
                        .round(3, 0)
                        .toNumber(),
                      avg_bet: new Big(prevDataTotalBet)
                        .div(prevAmountPlayed || 1)
                        .round(3, 0)
                        .toNumber(),
                    },
                  };
                }),
              );

              this.leaderboard.update(updates, [dimension.name]);
            },
          ),
        );
      } else {
        throw new Error('No round data available yet');
      }
    } catch (e) {
      console.log(e);
      Logger.error(e);
    }
  }

  async deleteOld() {
    const features = ['amount_played'];
    const dimensions = ['best-month', 'best-week', 'best-day', 'best-hour'];

    await Promise.all(
      dimensions.map((dimension) =>
        Promise.all(
          features.map(async (feature) => {
            const leaderboard = this.leaderboard.getRawLeaderboard(
              dimension,
              feature,
            ) as PeriodicLeaderboard;

            console.log('Current', leaderboard.getKeyNow());

            await Promise.all(
              (
                await leaderboard.getExistingKeys()
              ).map((key) => {
                if (key !== leaderboard.getKeyNow()) {
                  const lb = leaderboard.getLeaderboard(key);
                  console.log('Deleting', key);
                  return lb.clear();
                }
              }),
            );
          }),
        ),
      ),
    );
  }
}
