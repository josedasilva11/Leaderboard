import { Controller, Get, Query } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('/leaderboard')
  getLeaderboard(
    @Query('dimension') dimension: string,
    @Query('feature') feature: string,
  ) {
    return this.appService.getLeaderboard(dimension, feature);
  }
}
