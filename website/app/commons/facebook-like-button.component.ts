import {Component} from 'angular2/core';

@Component({
  selector: 'facebook-like-button',
  template: `<div class="fb-like" [attr.data-href]="url" data-layout="standard" data-action="like" data-show-faces="false" data-share="true"></div>`
})
export class FacebookLikeButtonComponent {

  url: String;

  constructor() {
    this.url = window.location.href;
  }

  ngAfterContentInit() {
//    FB.XFBML.parse();
  }
}
